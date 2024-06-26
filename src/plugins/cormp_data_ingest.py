import logging
import os
import configparser
import queue
import sys
import time
from datetime import datetime, timedelta
import pandas as pd

if 'DEBUG' not in os.environ:
    sys.path.append('./commonfiles/python')
else:
    sys.path.append('../../commonfiles/python')
from xlrd import open_workbook, cellname

from dataclasses import dataclass
import requests
from ..base_data_ingest import BaseDataIngest
from xeniaSQLAlchemy import multi_obs, platform
from xenia_obs_map import obs_map, json_obs_map
from MultiProcDataSaverV2 import MPDataSaverV2
from multiprocessing import Queue, Process, current_process
from unitsConversion import uomconversionFunctions
from sqlalchemy import func

class CORMPException(Exception):
    pass
class DataRecord:
    def __init__(self, parameter_json):
        self._parameter = parameter_json
        self._observation_parts_keys = list(self._parameter['observations'].keys())
        self._current_index = 0
    @property
    def name(self):
        return self._parameter['id']
    @property
    def units(self):
        return self._parameter['units']
    def __iter__(self):
        return self
    def __next__(self):
        if self._current_index < len(self._parameter['observations'][self._observation_parts_keys[0]]):
            result = {key: self._parameter['observations'][key][self._current_index] for key in self._observation_parts_keys}
            self._current_index += 1
            return result
        else:
            raise StopIteration

class DataRecords:
    def __init__(self, json: {}):
        self._json_data = json
        self._parameters_index = None
    def build_parameters_index(self):
        self._parameters_index = {}
        for ndx, parameter in enumerate(self._json_data):
            param_name = parameter['id']
            self._parameters_index[param_name] = ndx

    def get_parameter_data(self, parameter_name: str):
        if self._parameters_index is None:
            self.build_parameters_index()
        if parameter_name in self._parameters_index:
            param_ndx = self._parameters_index[parameter_name]
            data_rec = DataRecord(self._json_data[param_ndx])
            return data_rec
        return None

class Platform:
    def __init__(self, json: {}):
        self._platform_json = json
        self._data_records = None
        if "parameters" in self._platform_json['properties']:
            self._data_records = DataRecords(self._platform_json['properties']['parameters'])

    @property
    def location(self):
        '''
        :return:
            Returns a tuple with longitude,latitude of the platform if it is a Point, otherwise just returns the
        coordinates.

        '''
        if 'geometry' in self._platform_json:
            if self._platform_json['geometry']['type'] == 'Point':
                return(self._platform_json['geometry']['coordinates'][0], self._platform_json['geometry']['coordinates'][1])
            else:
                return (self._platform_json['geometry']['coordinates'])

    @property
    def name(self):
        return self._platform_json['properties']['platform_id']
    @property
    def description(self):
        return self._platform_json['properties']['platform_description']

    @property
    def quality_level_definitions(self):
        return self._platform_json['properties']['quality_levels']


    def get_parameter_data(self, parameter_name: str):
        if self._data_records is not None:
            return self._data_records.get_parameter_data(parameter_name)
class CORMPApi:
    def __init__(self, base_url='http://services.cormp.org/data.php', logger_name=" "):
        self._base_url = base_url
        self._logger = logging.getLogger(logger_name)
        self._records = []
        return
    def __iter__(self):
        for rec in self._records:
            yield rec

    def record_count(self):
        return len(self._records)

    def get(self, **kwargs):
        params = {}
        #?format=json&platform={platform_name}&time={start_time}/{end_time}
        platform = kwargs.get('platform', None)
        begin_date = kwargs.get('begin_date', None)
        end_date = kwargs.get('end_date', None)
        format = kwargs.get('format', 'json')

        if platform:
            params['platform'] = platform
        else:
            raise CORMPException('Missing station parameter')

        if begin_date and end_date:
            params['time'] = f"{begin_date}/{end_date}"
        else:
            raise CORMPException('Missing begin_date and/or end_date parameter')

        params['format'] = 'json'
        if format:
            params['format'] = format
        try:
            req = requests.get(self._base_url, params=params)
            if req.status_code == 200:
                if format == 'json':
                    return self.parse_json_response(req.json())
            else:
                self._logger.error(f"Request failed. Code: {req.status_code} Reason: {req.reason}")
        except Exception as e:
            raise e
        return None

    def parse_json_response(self, json_req):
        if json_req['type'] == 'Feature':
            platform = Platform(json_req)
        elif json_req['type'] == 'FeatureCollection':
            platform = Platform(json_req['features'][0])

        return platform

def processing_function(**kwargs):
    process_files = True
    start_time = time.time()
    try:
        base_logfile_name = kwargs['base_logfile_name']

        input_queue = kwargs["input_queue"]
        output_queue = kwargs["output_queue"]
        rest_base_url = kwargs["rest_base_url"]
        obs_mapping_file = kwargs["obs_mapping_file"]
        db_user = kwargs["db_user"]
        db_pwd = kwargs["db_pwd"]
        db_host = kwargs["db_host"]
        db_name = kwargs["db_name"]
        db_connection_type = kwargs["db_connection_type"]

        filename_parts = os.path.split(base_logfile_name)
        filename, ext = os.path.splitext(filename_parts[1])

        worker_filename = os.path.join(filename_parts[0],
                                       f"cormp_{filename}_{current_process().name.replace(':', '_')}{ext}")

        logging_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'default_for_cormp_processing_function': {
                    'format': "%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s",
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'handlers': {
                'stream': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'default_for_cormp_processing_function',
                    'level': logging.DEBUG
                },
                'file_handler': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': worker_filename,
                    'formatter': 'default_for_cormp_processing_function',
                    'level': logging.DEBUG,
                    'maxBytes': 10000000,
                    'backupCount': 5

                }
            },
            'loggers': {
                'cormp_processing_function': {
                    'handlers': ['stream', 'file_handler'],
                    'level': logging.DEBUG
                    #'propagate': True
                }
            }
        }
        logging.config.dictConfig(logging_config)
        logger = logging.getLogger("cormp_processing_function")
        logger.setLevel(logging.DEBUG)
        logger.debug(f"{current_process().name} starting run.")

        while(process_files):
            row_entry_date = datetime.now()
            platform_info = input_queue.get()
            if platform_info is not None:
                start_date = platform_info['start_date']
                platform_handle = platform_info['platform_handle']
                geometry = platform_info['geometry']
                platform_name_parts = platform_handle.split('.')
                logger.info(f"{current_process().name} processing platform: {platform_handle}")

                json_obs = json_obs_map()
                json_obs.load_json_mapping(obs_mapping_file)
                json_obs.build_db_mappings(platform_handle=platform_handle,
                                           db_connectionstring=db_connection_type,
                                           db_user=db_user,
                                           db_password=db_pwd,
                                           db_host=db_host,
                                           db_name=db_name)
                try:
                    end_date = datetime.utcnow()
                    start_date = start_date - timedelta(hours=2)
                    start_datetime = start_date.strftime('%Y-%m-%dT%H:%M:%S')
                    end_datetime = end_date.strftime('%Y-%m-%dT%H:%M:%S')

                    processed_wind = False
                    api = CORMPApi(base_url=rest_base_url, logger_name="cormp_processing_function")

                    org, platform_name, platform_type = platform_handle.split('.')

                    logger.info(f"Platform query : {platform_name} "
                                f"{start_datetime} to {end_datetime} ")

                    platform_data = api.get(platform=platform_name,
                                            begin_date=start_datetime,
                                            end_date=end_datetime,
                                            format='json')

                    if platform_data is not None:
                        for xenia_obs_rec in json_obs:
                            sensor_id = xenia_obs_rec.sensor_id
                            m_type_id = xenia_obs_rec.m_type_id
                            parameter_data = platform_data.get_parameter_data(xenia_obs_rec.source_obs)
                            if parameter_data is not None:
                                for data_rec in parameter_data:
                                    try:
                                        value = float(data_rec['values'])
                                    except TypeError as e:
                                        logger.exception(e)
                                    else:
                                        obs_rec = multi_obs(row_entry_date=row_entry_date.strftime("%Y-%m-%dT%H:%M:%S"),
                                                            m_date=data_rec['times'],
                                                            platform_handle=platform_handle,
                                                            sensor_id=sensor_id,
                                                            m_type_id=m_type_id,
                                                            m_lon=platform_info['geometry'][0],
                                                            m_lat=platform_info['geometry'][1],
                                                            m_value=value
                                                            )
                                        logger.debug(f"{platform_handle} queueing obs: {xenia_obs_rec.source_obs} "
                                                     f"datetime: {obs_rec.m_date} value: {obs_rec.m_value}")
                                        output_queue.put(obs_rec)
                except Exception as e:
                    logger.exception(e)
            else:
                logger.info(f"{current_process().name} end of input queue, terminating process. "
                            f"{time.time() - start_time} seconds.")
                process_files = False

    except Exception as e:
        logger.exception(e)
    return

@dataclass
class DataIngest(BaseDataIngest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._organization_name = ['cormp','carocoops']
        self._download_directory = ''
        self._obs_mapping_file = ''
        self._input_queue = Queue()
        self._data_queue = Queue()
        self._data_saver_queue = Queue()
        self._logging_queue = Queue()
        self._data_saver = None
        self._file_url = ''
        self._worker_count = ''
        self._logging_config = None
        self._ini_file = None

    def initialize(self, **kwargs):
        try:
            super().initialize(**kwargs)

            config_file = configparser.RawConfigParser()
            config_file.read(self._ini_file)

            self._base_url = config_file.get('settings', 'base_url')
            self._units_conversion_file = config_file.get('settings', 'units_conversion')
            db_settings_ini = config_file.get('Database', 'ini_filename')
            self._worker_count = config_file.getint('workers', 'count')
            self._platforms = config_file.get('platforms', 'handles').split(",")
            self._obs_mapping_file = config_file.get('platforms', 'obs_mapping_file')
            self.setup_database(db_settings_ini)
            self._data_saver = MPDataSaverV2()

            self._data_saver.initialize(data_queue=self._data_saver_queue,
                                        log_config=self._logging_config,
                                        db_user=self._db_user,
                                        db_pwd=self._db_pwd,
                                        db_host=self._db_host,
                                        db_name=self._db_name,
                                        db_connection_type=self._db_connection_type,
                                        records_before_commit=1)

            return True
        except Exception as e:
            raise e
        return False

    def setup_database(self, ini_file):
        try:
            db_config_file = configparser.RawConfigParser()
            db_config_file.read(ini_file)
            self._db_user = db_config_file.get('Database', 'user')
            self._db_pwd = db_config_file.get('Database', 'password')
            self._db_host = db_config_file.get('Database', 'host')
            self._db_name = db_config_file.get('Database', 'name')
            self._db_connection_type = db_config_file.get('Database', 'connectionstring')
            self.connect_to_database(db_user=self._db_user,
                                     db_pwd=self._db_pwd,
                                     db_host=self._db_host,
                                     db_name=self._db_name,
                                     db_connection_type=self._db_connection_type)
        except Exception as e:
            raise e

    def process_data(self, **kwargs):
        self._logger.info(f"process_data for {self._organization_name}")

        # Download the XLS file.
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=1)

        self._data_saver.start()

        for platform_handle in self._platforms:
            try:
                self._logger.debug(f"Querying platform metadata: {platform_handle}")
                platform_rec = self._db.session.query(platform)\
                    .filter(platform.platform_handle == platform_handle)\
                    .one()
            except Exception as e:
                self._logger.exception(e)
            else:
                # Build a list of data files to try and download to process.
                self._logger.debug(f"Processing platform: {platform_rec.platform_handle}")
                # Get the max date current in DB for platform.
                max_data_rec = self._db.session.query(func.max(multi_obs.m_date)) \
                    .filter(multi_obs.platform_handle == platform_rec.platform_handle) \
                    .all()
                if max_data_rec[0][0] is None:
                    # If there is no latest date, let's make it the past 24 hours.
                    start_date = datetime.now() - timedelta(hours=24)
                else:
                    start_date = max_data_rec[0][0]
                self._logger.debug(f"Platform: {platform_rec.platform_handle} Start Date: {start_date}")
                self._input_queue.put({"start_date": start_date,
                                       "platform_handle": platform_rec.platform_handle,
                                       "geometry": (platform_rec.fixed_longitude, platform_rec.fixed_latitude)})

        processes = []
        process_args = {
            "base_logfile_name": self._log_file,
            "logging_config": self._logging_config,
            "input_queue": self._input_queue,
            "output_queue": self._data_queue,
            "rest_base_url": self._base_url,
            "obs_mapping_file": self._obs_mapping_file,
            "db_user": self._db_user,
            "db_pwd": self._db_pwd,
            "db_host": self._db_host,
            "db_name": self._db_name,
            "db_connection_type": self._db_connection_type
        }
        for worker_num in range(self._worker_count):
            p = Process(target=processing_function, kwargs=process_args)
            self._logger.debug(f"Starting process: {p._name}")
            p.start()
            processes.append(p)
            self._input_queue.put(None)

        rec_count = 0
        while any([(check_job is not None and check_job.is_alive()) for check_job in processes]):
            if not self._data_queue.empty():
                if (rec_count % 10) == 0:
                    self._logger.debug(f"Processed {rec_count} results")
                self._data_saver.add_records([self._data_queue.get()])
                rec_count += 1
        self._logger.info(f"{self._worker_count} processes finished.")
        while not self._data_queue.empty():
            self._logger.debug("Pulling records from data_queue.")
            self._data_saver.add_records([self._data_queue.get()])
            rec_count += 1


        self.finalize()
        return

    def finalize(self):
        self._logger.info("Finalizing the plugin.")
        self._data_saver_queue.put(None)
        # Now shutdown the data saver.
        self._data_saver.join()
        self._logger.info("Finalized the plugin.")

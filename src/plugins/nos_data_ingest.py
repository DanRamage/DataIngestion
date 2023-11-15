import logging
import os
import configparser
import sys
import time
from datetime import datetime, timedelta
sys.path.append('../../commonfiles/python')
from dataclasses import dataclass
import requests
from ..base_data_ingest import BaseDataIngest
from sqlalchemy import func
from xeniaSQLAlchemy import multi_obs
from xenia_obs_map import obs_map, json_obs_map
from MultiProcDataSaverV2 import MPDataSaverV2
from multiprocessing import Queue, Process, current_process

NOSProducts = ["water_level", "hourly_height", "high_low", "daily_mean", "monthly_mean", "one_minute_water_level",
               "predictions", "datums", "air_gap", "air_temperature", "water_temperature", "wind", "air_pressure",
               "conductivity", "visibility", "humidity", "salinity", "currents", "currents_predictions"]

class NOSException(Exception):
    pass

#https://api.tidesandcurrents.noaa.gov/api/prod/
class NOSDataRecord:
    def __init__(self, id, name, latitude, longitude, date_time, product, units, value):
        self.id = id
        self.name = name
        self.longitude = longitude
        self.latitude = latitude
        self.date_time = date_time
        self.product_name = product
        self.value = value
        self.units = units

class NOSApi:
    def __init__(self, base_url='https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'):
        self._base_url = base_url
        self._logger = logging.getLogger()
        self._records = []
        return
    def __iter__(self):
        for rec in self._records:
            yield rec

    def record_count(self):
        return len(self._records)

    def get(self, **kwargs):
        params = {}
        station = kwargs.get('station', None)
        begin_date = kwargs.get('begin_date', None)
        end_date = kwargs.get('end_date', None)
        range = kwargs.get('range', None)
        product = kwargs.get('product', None)
        units = kwargs.get('units', 'metric')
        time_zone = kwargs.get('time_zone', 'gmt')
        datum = kwargs.get('datum', 'mllw')
        format = kwargs.get('format', 'json')

        params['time_zone'] = time_zone
        params['datum'] = datum
        params['format'] = format
        params['units'] = units

        if station:
            params['station'] = station
        else:
            raise NOSException('Missing station parameter')

        if begin_date:
            params['begin_date'] = begin_date
        else:
            raise NOSException('Missing begin_date parameter')
        if end_date:
            params['end_date'] = end_date
        else:
            raise NOSException('Missing end_date parameter')
        if range:
            params['range'] = range
        else:
            if begin_date is None or end_date is None:
                raise NOSException('Missing range parameter')
        if product:
            params['product'] = product
        else:
            raise NOSException('Missing product parameter')
        try:
            req = requests.get(self._base_url, params=params)
            if req.status_code == 200:
                if format == 'json':
                    self.parse_json_response(req.json(), product, units)
            else:
                self._logger.error(f"Request failed. Code: {req.status_code} Reason: {req.reason}")
        except Exception as e:
            raise e
        return

    def parse_json_response(self, json_req, product, units):
        if 'error' not in json_req:
            for rec in json_req['data']:
                date_time = datetime.strptime(rec['t'], '%Y-%m-%d %H:%M')
                #(self, id, name, latitude, longitude, date_time, product, units, value)
                if product == 'wind':
                    try:
                        value = float(rec['s'])
                    except ValueError as e:
                        e
                    else:
                        data_rec = NOSDataRecord(json_req['metadata']['id'],
                                                 json_req['metadata']['name'],
                                                 json_req['metadata']['lat'],
                                                 json_req['metadata']['lon'],
                                                 date_time,
                                                 'wind_speed',
                                                 units,
                                                 value)
                    self._records.append(data_rec)
                    try:
                        value = float(rec['g'])
                    except ValueError as e:
                        e
                    else:
                        data_rec = NOSDataRecord(json_req['metadata']['id'],
                                                 json_req['metadata']['name'],
                                                 json_req['metadata']['lat'],
                                                 json_req['metadata']['lon'],
                                                 date_time,
                                                 'wind_gust',
                                                 units,
                                                 value)
                    self._records.append(data_rec)
                    try:
                        value = float(rec['d'])
                    except ValueError as e:
                        e
                    else:
                        data_rec = NOSDataRecord(json_req['metadata']['id'],
                                                 json_req['metadata']['name'],
                                                 json_req['metadata']['lat'],
                                                 json_req['metadata']['lon'],
                                                 date_time,
                                                 'wind_from_direction',
                                                 units,
                                                 value)
                    self._records.append(data_rec)

                else:
                    try:
                        value = float(rec['v'])
                    except ValueError as e:
                        e
                    else:
                        data_rec = NOSDataRecord(json_req['metadata']['id'],
                                                 json_req['metadata']['name'],
                                                 json_req['metadata']['lat'],
                                                 json_req['metadata']['lon'],
                                                 date_time,
                                                 product,
                                                 units,
                                                 value)
                    self._records.append(data_rec)
        else:
            raise NOSException(json_req['error']['message'])
        return

def processing_function(**kwargs):
    process_files = True
    start_time = time.time()
    try:
        logging_config = kwargs['logging_config']
        input_queue = kwargs["input_queue"]
        output_queue = kwargs["output_queue"]
        rest_base_url = kwargs["rest_base_url"]
        obs_mapping_file = kwargs["obs_mapping_file"]
        db_user = kwargs["db_user"]
        db_pwd = kwargs["db_pwd"]
        db_host = kwargs["db_host"]
        db_name = kwargs["db_name"]
        db_connection_type = kwargs["db_connection_type"]

        logger_config = logging_config
        # Each worker will set its own filename for the filehandler
        base_filename = logger_config['handlers']['file_handler']['filename']
        filename_parts = os.path.split(base_filename)
        filename, ext = os.path.splitext(filename_parts[1])
        worker_filename = os.path.join(filename_parts[0], f"{filename}_{current_process().name.replace(':', '_')}{ext}")
        logger_config['handlers']['file_handler']['filename'] = worker_filename
        logging.config.dictConfig(logger_config)
        logger = logging.getLogger()
        logger.debug(f"{current_process().name} starting data saver worker.")

        logger = logging.getLogger()
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
                    start_datetime = start_date.strftime("%Y%m%d% %H:%M")
                    end_datetime = end_date.strftime("%Y%m%d% %H:%M")

                    processed_wind = False
                    for xenia_obs_rec in json_obs:
                        try:
                            do_request = True
                            if xenia_obs_rec.source_obs == 'wind':
                                if not processed_wind:
                                    processed_wind = True
                                    do_request = True
                                else:
                                    do_request = False

                            nos_req = NOSApi()
                            if do_request:
                                logger.info(f"Platform query : {platform_name_parts[1]} "
                                            f"{start_datetime} to {end_datetime} Product: {xenia_obs_rec.source_obs}")
                                nos_req.get(station=platform_name_parts[1],
                                            begin_date=start_datetime,
                                            end_date=end_datetime,
                                            product=xenia_obs_rec.source_obs,
                                            datum='MLLW',
                                            time_zone='gmt',
                                            units='metric',
                                            format='json'
                                            )
                                logger.info(f"Platform query : {platform_name_parts[1]} "
                                            f"{start_datetime} to {end_datetime} Product: {xenia_obs_rec.source_obs}"
                                            f"returned: {nos_req.record_count()} records.")

                            for rec in nos_req:
                                #We have to look up the obs_info again since some products like wind
                                #return multiple records(wind_speed, wind_gust, wind_direction).
                                obs_info = json_obs.get_rec_from_xenia_name(rec.product_name)
                                sensor_id = obs_info.sensor_id
                                m_type_id = obs_info.m_type_id

                                obs_rec = multi_obs(row_entry_date=row_entry_date.strftime("%Y-%m-%dT%H:%M:%S"),
                                                    m_date=rec.date_time.strftime("%Y-%m-%dT%H:%M:%S"),
                                                    platform_handle=platform_handle,
                                                    sensor_id=sensor_id,
                                                    m_type_id=m_type_id,
                                                    m_lon=rec.longitude,
                                                    m_lat=rec.latitude,
                                                    m_value=rec.value
                                                    )
                                output_queue.put(obs_rec)
                        except Exception as e:
                            logger.exception(e)
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
        self._organization_name = 'nos'
        self._obs_mapping_file = ''
        self._input_queue = Queue()
        self._data_queue = Queue()
        self._data_saver_queue = Queue()
        self._data_saver = None
        self._worker_count = 1
        self._logging_config = None
        self._base_url = ''

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

    def initialize(self, **kwargs):
        try:
            ini_filename = os.path.join(self._plugin_path, f"{self._plugin_name}.ini")
            config_file = configparser.RawConfigParser()
            config_file.read(ini_filename)

            log_file = config_file.get('logging', 'log_file')

            self._logging_config = {
                'version': 1,
                'disable_existing_loggers': False,
                'formatters': {
                    'f': {
                        'format': "%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s",
                        'datefmt': '%Y-%m-%d %H:%M:%S'
                    }
                },
                'handlers': {
                    'stream': {
                        'class': 'logging.StreamHandler',
                        'formatter': 'f',
                        'level': logging.DEBUG
                    },
                    'file_handler': {
                        'class': 'logging.handlers.RotatingFileHandler',
                        'filename': log_file,
                        'formatter': 'f',
                        'level': logging.DEBUG
                    }
                },
                'root': {
                    'handlers': ['file_handler', 'stream'],
                    'level': logging.NOTSET,
                    'propagate': False
                }
            }
            logging.config.dictConfig(self._logging_config)
            self._logger = logging.getLogger()
            self._logger.info("Logging configured.")
            self._obs_mapping_file = config_file.get('obs_mapping', 'file')
            self._rest_base_url = config_file.get('webservice', 'base_url')
            db_settings_ini = config_file.get('Database', 'ini_filename')
            self._worker_count = config_file.getint('workers', 'count')

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

    def process_data(self):
        self._logger.info(f"process_data for {self._organization_name}")
        self._data_saver.start()
        #Get the NOS platforms we wnt to get data for.
        platform_recs = self.get_platforms(self._organization_name)
        #Build a list of data files to try and download to process.
        for platform_rec in platform_recs:
            #Get the max date current in DB for platform.
            max_data_rec = self._db.session.query(func.max(multi_obs.m_date))\
                .filter(multi_obs.platform_handle == platform_rec.platform_handle)\
                .all()
            if max_data_rec[0][0] is None:
                #If there is no latest date, let's make it the past 72 hours.
                start_date = datetime.now() - timedelta(hours=24)
            else:
                start_date = max_data_rec[0][0]
            self._input_queue.put({"start_date": start_date,
                                   "platform_handle": platform_rec.platform_handle,
                                   "geometry": (platform_rec.fixed_longitude, platform_rec.fixed_latitude)})

        processes = []
        process_args = {
            "logging_config": self._logging_config,
            "input_queue": self._input_queue,
            "output_queue": self._data_queue,
            "rest_base_url": self._rest_base_url,
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
        #Now shutdown the data saver.
        self._data_saver.join()
        self._logger.info("Finalized the plugin.")


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
from dataclasses import dataclass
import requests
from ..base_data_ingest import BaseDataIngest
import urllib.parse
from sqlalchemy import func
from xeniaSQLAlchemy import multi_obs
from xenia_obs_map import obs_map, json_obs_map
from MultiProcDataSaverV2 import MPDataSaverV2
from multiprocessing import Queue, Process, current_process




@dataclass
class NDBCTextBase:
    def __init__(self):
        self._logger = logging.getLogger()
        self._download_file = ""

    def get_file(self, ndbc_file_url, download_directory):
        try:
            req = requests.get(ndbc_file_url)
        except Exception as e:
            raise e
        else:
            url_parts = urllib.parse.urlsplit(ndbc_file_url)
            filename = os.path.split(url_parts[2])
            if req.status_code == 200:
                self._download_file = os.path.join(download_directory, filename[-1])
                with open(self._download_file, "w") as data_file:
                    for chunk in req.iter_content(chunk_size=1024):
                        data_file.write(chunk.decode('UTF-8'))
                    return True
            else:
                self._logger.error(f"Unable to download file: {filename}. Code: {req.status_code} Reason: {req.reason}")
        return False

    def process(self):
        return

'''
WDIR
    Wind direction (the direction the wind is coming from in degrees clockwise from true N) during the same period used for WSPD. See Wind Averaging Methods
WSPD
    Wind speed (m/s) averaged over an eight-minute period for buoys and a two-minute period for land stations. Reported Hourly. See Wind Averaging Methods.
GST
    Peak 5 or 8 second gust speed (m/s) measured during the eight-minute or two-minute period. The 5 or 8 second period can be determined by payload, See the Sensor Reporting, Sampling, and Accuracy section.
WVHT
    Significant wave height (meters) is calculated as the average of the highest one-third of all of the wave heights during the 20-minute sampling period. See the Wave Measurements section.
DPD
    Dominant wave period (seconds) is the period with the maximum wave energy. See the Wave Measurements section.
APD
    Average wave period (seconds) of all waves during the 20-minute period. See the Wave Measurements section.
MWD
    The direction from which the waves at the dominant period (DPD) are coming. The units are degrees from true North, increasing clockwise, with North as 0 (zero) degrees and East as 90 degrees. See the Wave Measurements section.
PRES
    Sea level pressure (hPa). For C-MAN sites and Great Lakes buoys, the recorded pressure is reduced to sea level using the method described in NWS Technical Procedures Bulletin 291 (11/14/80). ( labeled BAR in Historical files)
ATMP
    Air temperature (Celsius). For sensor heights on buoys, see Hull Descriptions. For sensor heights at C-MAN stations, see C-MAN Sensor Locations
WTMP
    Sea surface temperature (Celsius). For buoys the depth is referenced to the hull's waterline. For fixed platforms it varies with tide, but is referenced to, or near Mean Lower Low Water (MLLW).
DEWP
    Dewpoint temperature taken at the same height as the air temperature measurement.
VIS
    Station visibility (nautical miles). Note that buoy stations are limited to reports from 0 to 1.6 nmi.
PTDY
    Pressure Tendency is the direction (plus or minus) and the amount of pressure change (hPa)for a three hour period ending at the time of observation. (not in Historical files)
TIDE
    The water level in feet above or below Mean Lower Low Water (MLLW). 
'''
@dataclass
class NDBCMetRecord:
    def __init__(self, series_header, pd_series):
        #Build the date and time.
        date = f"{pd_series['#YY'][0]}-{pd_series['MM'][0]}-{pd_series['DD'][0]}"
        time = f"{pd_series['hh'][0]}:{pd_series['mm'][0]}"
        self.date_time = datetime.strptime(f"{date}T{time}", "%Y-%m-%dT%H:%M")
        #First 5 columns are date/time.
        for obs in series_header[5:]:
            value_units = (pd_series[obs[0]][0], pd_series[obs[0]].index[0])
            setattr(self, obs[0], value_units)
        return

@dataclass
class NDBCMet(NDBCTextBase):
    def __init__(self):
        super().__init__()
        self._data_frame = None
        self._header_rows = []
        return
    def __iter__(self):
        for index, row in self._data_frame.iterrows():
            if index > 2:
                met_rec = NDBCMetRecord(self._data_frame.columns.values, row)
                yield (index, met_rec)

    def __next__(self):
        for index, row in self._data_frame.iterrows():
            met_rec = NDBCMetRecord(self._data_frame.columns.values, row)
            yield (index, met_rec)

    def process(self, ndbc_file_url, download_directory):
        try:
            if self.get_file(ndbc_file_url, download_directory):
                self._data_frame = pd.read_csv(self._download_file, sep=r"\s+", header=[0,1])
                return True
            else:
                return False
        except Exception as e:
            raise e


def processing_function(**kwargs):
    process_files = True
    try:
        start_time = time.time()
        #logging_config = kwargs['logging_config']
        base_logfile_name = kwargs['base_logfile_name']
        input_queue = kwargs["input_queue"]
        output_queue = kwargs["output_queue"]
        download_directory = kwargs["download_directory"]
        obs_mapping_file = kwargs["obs_mapping_file"]
        db_user = kwargs["db_user"]
        db_pwd = kwargs["db_pwd"]
        db_host = kwargs["db_host"]
        db_name = kwargs["db_name"]
        db_connection_type = kwargs["db_connection_type"]


        filename_parts = os.path.split(base_logfile_name)
        filename, ext = os.path.splitext(filename_parts[1])

        worker_filename = os.path.join(filename_parts[0],
                                       f"noaa_{filename}_{current_process().name.replace(':', '_')}{ext}")

        logging_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'default_for_ndbc_processing_function': {
                    'format': "%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s",
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'handlers': {
                'stream': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'default_for_ndbc_processing_function',
                    'level': logging.DEBUG
                },
                'file_handler': {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': worker_filename,
                    'formatter': 'default_for_ndbc_processing_function',
                    'level': logging.DEBUG
                }
            },
            'loggers': {
                'ndbc_processing_function': {
                    'handlers': ['stream', 'file_handler'],
                    'level': logging.DEBUG
                    #'propagate': True
                }
            }
        }
        logging.config.dictConfig(logging_config)
        logger = logging.getLogger("ndbc_processing_function")
        logger.setLevel(logging.DEBUG)
        '''
        logger = logging.getLogger("ndbc_processing_function")
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s,%(levelname)s,%(funcName)s,%(lineno)d,%(message)s")
        fh = logging.handlers.RotatingFileHandler(worker_filename)
        ch = logging.StreamHandler()
        fh.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        '''
        '''
        logger_config = logging_config
        # Each worker will set its own filename for the filehandler
        base_filename = "./mp_logger.log"
        file_handler_name = [handler for handler in logger_config['handlers'] if 'file_handler' in handler]
        if len(file_handler_name):
            base_filename = logger_config['handlers'][file_handler_name[0]]['filename']

        filename_parts = os.path.split(base_filename)
        filename, ext = os.path.splitext(filename_parts[1])
        worker_filename = os.path.join(filename_parts[0], f"{filename}_{current_process().name.replace(':', '_')}{ext}")
        logger_config['handlers'][file_handler_name[0]]['filename'] = worker_filename
        logging.config.dictConfig(logger_config)
        '''

        #logger = logging.getLogger("ndbc_processing_function")
        logger.debug(f"{current_process().name} starting run.")

        while(process_files):
            row_entry_date = datetime.now()
            platform_info = input_queue.get()
            if platform_info is not None:
                start_date = platform_info['start_date']
                platform_file = platform_info['url']
                platform_handle = platform_info['platform_handle']
                geometry = platform_info['geometry']

                logger.info(f"{current_process().name} processing platform: {platform_handle}")

                json_obs = json_obs_map()
                json_obs.load_json_mapping(obs_mapping_file)
                json_obs.build_db_mappings(platform_handle=platform_handle,
                                           db_connectionstring=db_connection_type,
                                           db_user=db_user,
                                           db_password=db_pwd,
                                           db_host=db_host,
                                           db_name=db_name)

                ndbc_met = NDBCMet()
                logger.info(f"{current_process().name} getting file: {platform_file}")
                if ndbc_met.process(platform_file, download_directory):
                    for index, met_rec in ndbc_met:
                        if index % 100:
                            logger.debug(f"Processing row: {index} Record Date: {met_rec.date_time} "
                                         f"Start Date: {start_date}")
                        if met_rec:
                            #Let's only process the records from the start_date forward.
                            if start_date is not None:
                                if met_rec.date_time < start_date:
                                    break
                            for xenia_obs_rec in json_obs:
                                value, units = getattr(met_rec, xenia_obs_rec.source_obs)
                                try:
                                    value = float(value)
                                except ValueError as e:
                                    logger.error(f"Platform: {platform_handle} {met_rec.date_time} "
                                                 f"Obs: {xenia_obs_rec.source_obs} Value: {value} is not a number.")
                                    #logger.exception(e)
                                else:
                                    logger.debug(f"Platform: {platform_handle} {met_rec.date_time} "
                                                 f"Obs: {xenia_obs_rec.target_obs} Value: {value}")
                                    obs_rec = multi_obs(row_entry_date=row_entry_date.strftime("%Y-%m-%dT%H:%M:%S"),
                                                        m_date=met_rec.date_time,
                                                        platform_handle=platform_handle,
                                                        sensor_id=xenia_obs_rec.sensor_id,
                                                        m_type_id=xenia_obs_rec.m_type_id,
                                                        m_lon=geometry[0],
                                                        m_lat=geometry[1],
                                                        m_value=value
                                                        )
                                    try:
                                        output_queue.put(obs_rec)
                                    except (queue.Empty, Exception) as e:
                                        logger.debug("Queue full.")
                        else:
                            logger.error("met_rec is None")
            else:
                logger.info(f"{current_process().name} end of input queue, terminating process. "
                            f"{time.time()-start_time} seconds.")
                process_files = False
                break
    except Exception as e:
        logger.exception(e)

    return

@dataclass
class DataIngest(BaseDataIngest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._organization_name = 'ndbc'
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

    def initialize(self, **kwargs):
        try:
            super().initialize(**kwargs)

            config_file = configparser.RawConfigParser()
            config_file.read(self._ini_file)

            self._file_url = config_file.get('text_files', 'url')
            self._download_directory = config_file.get('text_files', 'download_directory')
            self._obs_mapping_file = config_file.get('obs_mapping', 'file')
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
                                        records_before_commit=10)
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

        #Check to make sure our download directory exists, and if not we create it.
        if not os.path.exists(self._download_directory):
            self._logger.debug(f"Creating download directory: {self._download_directory}")
            os.makedirs(self._download_directory, exist_ok=True)
        self._data_saver.start()
        #Get the NDBC platforms we wnt to get data for.
        platform_recs = self.get_platforms(self._organization_name)
        #Build a list of data files to try and download to process.
        for platform_rec in platform_recs:
            self._logger.debug(f"Processing platform: {platform_rec.platform_handle}")

            platform_file_url = urllib.parse.urljoin(self._file_url, f"{platform_rec.short_name}.txt")
            #Get the max date current in DB for platform.
            max_data_rec = self._db.session.query(func.max(multi_obs.m_date))\
                .filter(multi_obs.platform_handle == platform_rec.platform_handle)\
                .all()
            if max_data_rec[0][0] is None:
                #If there is no latest date, let's make it the past 72 hours.
                start_date = datetime.now() - timedelta(hours=72)
            else:
                start_date = max_data_rec[0][0]
            self._input_queue.put({"start_date": start_date,
                                   "url": platform_file_url,
                                   "platform_handle": platform_rec.platform_handle,
                                   "geometry": (platform_rec.fixed_longitude, platform_rec.fixed_latitude)})

        processes = []
        process_args = {
            "base_logfile_name": self._log_file,
            "logging_config": self._logging_config,
            "input_queue": self._input_queue,
            "output_queue": self._data_queue,
            "download_directory": self._download_directory,
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

        #self.processing_function()

        self.finalize()
        return

    def finalize(self):
        self._logger.info("Finalizing the plugin.")
        self._data_saver_queue.put(None)
        #Now shutdown the data saver.
        self._data_saver.join()
        self._logger.info("Finalized the plugin.")


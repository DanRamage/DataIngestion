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
import urllib.parse
from sqlalchemy import func
from xeniaSQLAlchemy import multi_obs, platform
from xenia_obs_map import obs_map, json_obs_map
from MultiProcDataSaverV2 import MPDataSaverV2
from multiprocessing import Queue, Process, current_process
from unitsConversion import uomconversionFunctions


class LBHMCBase:
    def __init__(self, ini_file: str, platform_name: str, logger_name: str):
        self._logger = logging.getLogger(logger_name)
        self._ini_file = ini_file
        self._platform = platform_name
        self._download_file = ""
        self._json_obs_file = None
        self._site_id = None
        self._sensor_list = None
        self._platform_handle = None
        self._file_name = None

    def initialize(self, db_connection_type: str, db_user: str, db_pwd: str, db_host: str, db_name: str):
        try:
            config_file = configparser.RawConfigParser()
            config_file.read(self._ini_file)

            self._json_obs_file = config_file.get(self._platform, "obs_mapping_file")
            self._site_id = config_file.get(self._platform, 'site_id')
            self._sensor_list = config_file.get(self._platform, 'sensor_ids')
            self._platform_handle = config_file.get(self._platform, 'platform_handle')
            self._file_name = config_file.get(self._platform, 'file_name')

            units_conversion_file = config_file.get('settings', 'units_conversion')
            self._uom_converter = uomconversionFunctions(units_conversion_file)

            self._json_obs = json_obs_map()
            self._json_obs.load_json_mapping(self._json_obs_file)
            self._json_obs.build_db_mappings(platform_handle=self._platform_handle,
                                             db_connectionstring=db_connection_type,
                                             db_user=db_user,
                                             db_password=db_pwd,
                                             db_host=db_host,
                                             db_name=db_name)
        except Exception as e:
            self._logger.exception(e)

    def get_file(self, file_url: str, start_time: str, end_time: str, now_time: str):
        logger = logging.getLogger(__name__)
        base_url = file_url
        try:
            request_params = {
                'action': 'Excel',
                'siteId': self._site_id,
                'sensorId': self._sensor_list,
                'startDate': start_time,
                'endDate': end_time,
                'displayType': 'StationSensor',
                'predefFlag': 'false',
                'enddateFlag': 'false',
                'now': now_time
            }
            payload_str = "&".join("%s=%s" % (k, v) for k, v in request_params.items())
            logger.debug("Request: %s params: %s" % (base_url, payload_str))
            req = requests.get(base_url, params=payload_str)
            if req.status_code == 200:
                logger.debug(f"Request successful, saving to file: {self._file_name}")
                with open(self._file_name, 'wb') as f:
                    for chunk in req.iter_content(1024):
                        f.write(chunk)
                    return True
            else:
                logger.error(f"Request failed, code: {req.status_code}")
        except Exception as e:
            logger.exception(e)
        return False

    def process(self, output_queue: Queue, db_obj):
        self._logger.debug(f"Opening file: {self._file_name}")
        try:
            wb = open_workbook(filename=self._file_name)
        except Exception as e:
            self._logger.exception(e)
        else:
            sheet = wb.sheet_by_index(0)
            # Get platform info for lat/long
            plat_rec = db_obj.session.query(platform) \
                .filter(platform.platform_handle == self._platform_handle) \
                .one()

            row_entry_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for row_index in range(sheet.nrows):
                try:
                    if row_index > 0:
                        # HEader row, add the column index so we can lookup the obs in the worksheet.
                        if row_index == 1:
                            for col_index in range(sheet.ncols):
                                field_name = sheet.cell(row_index, col_index).value
                                obs_rec = self._json_obs.get_rec_from_source_name(field_name)
                                if obs_rec is not None:
                                    obs_rec.source_index = col_index
                        else:
                            # Build the database records.
                            m_date_rec = self._json_obs.get_date_field()
                            for obs_rec in self._json_obs:
                                # Skip the date, not a true obs.
                                if obs_rec.target_obs != 'm_date':
                                    try:
                                        m_date = sheet.cell(row_index, m_date_rec.source_index).value
                                        value = float(sheet.cell(row_index, obs_rec.source_index).value)
                                        if obs_rec.target_uom != obs_rec.source_uom:
                                            value = self._uom_converter.measurementConvert(value, obs_rec.source_uom,
                                                                                           obs_rec.target_uom)
                                        db_rec = multi_obs(row_entry_date=row_entry_date,
                                                           platform_handle=self._platform_handle,
                                                           sensor_id=(obs_rec.sensor_id),
                                                           m_type_id=(obs_rec.m_type_id),
                                                           m_date=m_date,
                                                           m_lon=plat_rec.fixed_longitude,
                                                           m_lat=plat_rec.fixed_latitude,
                                                           m_value=value
                                                           )

                                        self._logger.debug("%s Queueing m_date: %s obs(%d): %s(%s): %f" % \
                                                           (self._platform_handle,
                                                            db_rec.m_date,
                                                            db_rec.sensor_id,
                                                            obs_rec.target_obs,
                                                            obs_rec.target_uom,
                                                            db_rec.m_value))
                                        output_queue.put(db_rec)
                                    except ValueError as e:
                                        self._logger.error("%s m_date: %s obs(%d): %s(%s) no value" % \
                                                           (self._platform_handle,
                                                            m_date,
                                                            obs_rec.sensor_id,
                                                            obs_rec.target_obs,
                                                            obs_rec.target_uom
                                                            ))
                except Exception as e:
                    self._logger.exception(e)

        return


@dataclass
class DataIngest(BaseDataIngest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._organization_name = 'lbhmc'
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
            self._station_list = config_file.get('settings', 'station_list').split(",")
            self._units_conversion_file = config_file.get('settings', 'units_conversion')
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
        now_time = datetime.now()
        now_str = now_time.strftime('%a %b %d %Y %H:%M:%S GMT-0400 (EDT)')
        start_time = (now_time - timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00')
        # Sutron does not do < end time, it's <= end time so we want to handle that
        # by making the minutes 00:59:00
        end_time = (datetime.strptime(start_time, '%Y-%m-%d %H:00:00') + timedelta(minutes=59)).strftime(
            '%Y-%m-%d %H:%M:%S')

        self._data_saver.start()

        for station in self._station_list:
            self._logger.info(f"Processing: {station}")
            lbhm_data = LBHMCBase(self._ini_file, station, self._logger_name)
            # (self, db_connection_type, db_user, db_pwd, db_host, db_name):
            lbhm_data.initialize(self._db_connection_type, self._db_user, self._db_pwd, self._db_host, self._db_name)
            if lbhm_data.get_file(self._base_url, start_time, end_time, now_str):
                lbhm_data.process(self._data_saver_queue, self._db)

        self.finalize()
        return

    def finalize(self):
        self._logger.info("Finalizing the plugin.")
        self._data_saver_queue.put(None)
        # Now shutdown the data saver.
        self._data_saver.join()
        self._logger.info("Finalized the plugin.")

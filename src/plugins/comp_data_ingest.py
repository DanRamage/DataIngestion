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
from .cormp_data_ingest import CORMPApi, processing_function

#http://comps.marine.usf.edu:81/services/data.php?format=json&pretty=true&time=2024-05-03T12%3A24%3A05-04%3A00%2F2024-05-07T12%3A00%3A00-04%3A00&platform=c10&standard=true&qcFilter=false&health=Off&region=&datum=MLLW&windPrediction=wind%20speed%20prediction&www=true&dataView=less&allStations=true
@dataclass
class DataIngest(BaseDataIngest):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._organization_name = ['comp']
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

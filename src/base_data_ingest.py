import sys
import os
sys.path.append('../commonfiles/python')

import logging
import configparser
from dataclasses import dataclass
import logging
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, organization, platform


@dataclass
class BaseDataIngest:
    def __init__(self, **kwargs):
        self._logger_name = kwargs.get('logger_name', '')
        self._logger = logging.getLogger(self._logger_name)
        self._output_queue = kwargs['output_queue']
        self._plugin_path = kwargs['module_path']
        self._plugin_name = self.__module__.split('.')[-1]

        #self._sqlite_file = kwargs.get("sqlite_file", None)
        #self._db_user = kwargs.get("db_user", None)
        #self._db_pwd = kwargs.get("db_pwd", None)
        #self._db_host = kwargs.get("db_host", None)
        #self._db_name = kwargs.get("db_name", None)
        #self._connection_type = kwargs.get("db_connection_type", None)
        self._sqlite_file = None
        self._db_user = None
        self._db_pwd = None
        self._db_host = None
        self._db_name = None
        self._db_connection_type = None
        self._db = None

        self._organization_name = ""
        self._organization_id = None
        return

    @property
    def plugin_name(self):
        return self._plugin_name
    def __del__(self):
        if self._db is not None:
            self._db.disconnect()

    def connect_to_database(self, **kwargs):
        sqlite_file = kwargs.get("sqlite_file", None)
        db_user = kwargs.get("db_user", None)
        db_pwd = kwargs.get("db_pwd", None)
        db_host = kwargs.get("db_host", None)
        db_name = kwargs.get("db_name", None)
        connection_type = kwargs.get("db_connection_type", None)

        if self._sqlite_file is None:
            self._db = xeniaAlchemy()
            if self._db.connectDB('postgresql', db_user, db_pwd, db_host, db_name, False):
                self._logger.debug(f"Connected to database: {db_name}")
            else:
                self._logger.error(f"Failed to connect to database: {db_name}")
        else:
            self._logger.error("Not implemented.")

    def get_platforms(self, organization_name):
        try:
            #Let's get a list of the platforms we want to retrieve the data for.
            platform_recs = self._db.session.query(platform).\
                join(organization,organization.row_id == platform.organization_id).\
                filter(organization.short_name == organization_name).\
                filter(platform.active <= 3).\
                order_by(platform.short_name).\
                all()
            self._logger.info("Organization: %s returned: %d platforms to query for data." % (organization_name, len(platform_recs)))
            return platform_recs
        except Exception as e:
            self._logger.exception(e)
        return None

    def initialize(self):
        try:
            if 'DEBUG' not in os.environ:
                ini_name = f"{self._plugin_name}"
            else:
                ini_name = f"{self._plugin_name}_debug"

            self._ini_file = os.path.join(self._plugin_path, f"{ini_name}.ini")
            config_file = configparser.RawConfigParser()
            config_file.read(self._ini_file)

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
        except Exception as e:
            raise e
        return
    def process_data(self):
        return
    def finalize(self):
        return

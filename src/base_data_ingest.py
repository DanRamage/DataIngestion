import sys
sys.path.append('../commonfiles/python')

import logging
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
        return
    def process_data(self):
        return
    def finalize(self):
        return

import connectorx as cx
from exceptions import ParamsMissingException

class Postgres():
    def __init__(self,config):
        self._pg_conn_str = f"postgresql://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._pg_conn_str = f"{self._pg_conn_str}/{config['DATABASE']}"

    def read_as_dataframe(self,query,database=None,return_type='pandas'):
        try:
            if self._dbname_in_config:
                return cx.read_sql(self._pg_conn_str, query,return_type=return_type)
            elif database:
                return cx.read_sql(f"{self._pg_conn_str}/{database}", query,return_type=return_type)
            else:
                raise ParamsMissingException(f"database parameter missing. Either add it in config or pass it as an argument.")
        except ParamsMissingException as e:
            print(e.message)

class MySQL():
    def __init__(self,config):
        self._mysql_conn_str = f"mysql://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._pg_conn_str = f"{self._pg_conn_str}/{config['DATABASE']}"

    def read_as_dataframe(self,query,database=None,return_type='pandas'):
        try:
            if self._dbname_in_config:
                return cx.read_sql(self._mysql_conn_str, query,return_type=return_type)
            elif database:
                return cx.read_sql(f"{self._mysql_conn_str}/{database}", query,return_type=return_type)
            else:
                raise ParamsMissingException(f"database parameter missing. Either add it in config file or pass it as an argument.")
        except ParamsMissingException as e:
            print(e.message)
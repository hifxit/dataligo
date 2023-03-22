import connectorx as cx

class Postgres():
    def __init__(self,config):
        self._pg_conn_str = f"postgresql://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._pg_conn_str = f"{self._pg_conn_str}/{config['DATABASE']}"

    def read_as_dataframe(self,query,database=None,return_type='pandas'):
        if self._dbname_in_config:
            return cx.read_sql(self._pg_conn_str, query,return_type=return_type)
        elif database:
            return cx.read_sql(f"{self._pg_conn_str}/{database}", query,return_type=return_type)
        else:
            print('Missing Params: database name')

class MySQL():
    def __init__(self,config):
        self._mysql_conn_str = f"mysql://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._pg_conn_str = f"{self._pg_conn_str}/{config['DATABASE']}"

    def read_as_dataframe(self,query,database=None,return_type='pandas'):
        if self._dbname_in_config:
            return cx.read_sql(self._mysql_conn_str, query,return_type=return_type)
        elif database:
            return cx.read_sql(f"{self._mysql_conn_str}/{database}", query,return_type=return_type)
        else:
            print('Missing Params: database name')
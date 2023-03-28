import connectorx as cx
import pandas as pd
import mariadb
from ..exceptions import ParamsMissingException

class DBCX():
    def __init__(self,config,db_type):
        self._conn_str = f"{db_type}://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._conn_str  = f"{self._conn_str}/{config['DATABASE']}"

    def read_as_dataframe(self,query,database=None,return_type='pandas'):
        if self._dbname_in_config:
            return cx.read_sql(self._conn_str , query,return_type=return_type)
        elif database:
            return cx.read_sql(f"{self._conn_str}/{database}", query,return_type=return_type)
        else:
            raise ParamsMissingException(f"database parameter missing. Either add it in config file or pass it as an argument.")

class Postgres(DBCX):
    def __init__(self,config):
        super().__init__(config,'postgresql')

class MySQL(DBCX):
    def __init__(self,config):
        super().__init__(config,'mysql')

class Oracle(DBCX):
    def __init__(self,config):
        super().__init__(config,'oracle')

class MsSQL(DBCX):
    def __init__(self,config):
        super().__init__(config,'mssql')

class Sqlite():
    def __init__(self,config):
        self._sqlite_conn = 'sqlite://' + config['DB_PATH']

    def read_as_dataframe(self,query, db_path=None,return_type='pandas'):
        if db_path:
            return cx.read_sql('sqlite://' + db_path, query, return_type=return_type)
        else:
            return cx.read_sql(self._sqlite_conn, query, return_type=return_type)


class MariaDB():
    def __init__(self,config):
        self._mdb_conn = mariadb.connect(
            user=config['USERNAME'],
            password=config['PASSWORD'],
            host=config['HOST'],
            port=config['PORT'],
            database=config['DATABASE']
        )

    def read_as_dataframe(self,query,return_type='pandas'):
        cur = self._mdb_conn.cursor()
        cur.execute(query)
        records = [row for row in cur]
        return pd.DataFrame(records)
    
    def close_connection(self):
        self._mdb_conn.close()
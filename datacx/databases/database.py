import connectorx as cx
import pandas as pd
import mariadb
from ..exceptions import ParamsMissingException
from ..datawarehouses.utils import _df_to_file_writer

class DBCX():
    """
    DBCX is the parent class for most of the Database, which use ConnectorX to read and download the data from the database.
    """
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
        
    def download_as_file(self,query: str, filename: str, database: str = None):
        df = self.read_as_dataframe(query=query, database=database, return_type='pandas')
        _df_to_file_writer(df, filename=filename)
        print('File saved to the path:', filename)

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
        """
        Sqlite class create sqlite dcx object to load data from sqlite database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._sqlite_conn = 'sqlite://' + config['DB_PATH']

    def read_as_dataframe(self, query: str, db_path=None,return_type='pandas'):
        """
        Takes query as argument and return a dataframe

        Args:
            query (str): select query
            db_path (str, optional): sqlite db file path (eg. /home/user/Desktop/my_sqlite.db). If None, It takes that from config file. Defaults to None.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'. Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
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
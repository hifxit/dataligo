import connectorx as cx
import pandas as pd
#import mariadb
from ..exceptions import ParamsMissingException
from ..datawarehouses.utils import _df_to_file_writer

class DBCX():
    def __init__(self,config,db_type):
        """
        DBCX is the parent class for most of the Database, which use ConnectorX to read and download the data from the database.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
            db_type (str): database type (eg: mysql, postgresql, etc). It is passed by the child class
        """
        self._conn_str = f"{db_type}://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._conn_str  = f"{self._conn_str}/{config['DATABASE']}"

    def read_as_dataframe(self,query: str,database: str = None,return_type='pandas'):
        """
        Takes query as argument and return dataframe

        Args:
            query (str): select query
            database (str, optional): database name, if None, it take it from config. Defaults to None.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'. Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        if self._dbname_in_config:
            return cx.read_sql(self._conn_str , query,return_type=return_type)
        elif database:
            return cx.read_sql(f"{self._conn_str}/{database}", query,return_type=return_type)
        else:
            raise ParamsMissingException(f"database parameter missing. Either add it in config file or pass it as an argument.")
        
    def download_as_file(self,query: str, filename: str, database: str = None):
        """
        Takes query as argument and download the data as file

        Args:
            query (str): select query
            filename (str): filename to save the file
            database (str, optional): database name, if None, it take it from config. Defaults to None.
        """
        df = self.read_as_dataframe(query=query, database=database, return_type='pandas')
        _df_to_file_writer(df, filename=filename)
        print('File saved to the path:', filename)

class Postgres(DBCX):
    def __init__(self,config):
        """
        Postgres class create postgresql dcx object to load data from postgresql database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'postgresql')

class MySQL(DBCX):
    def __init__(self,config):
        """
        MySQL class create mysql dcx object to load data from mysql database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'mysql')

class Oracle(DBCX):
    def __init__(self,config):
        """
        Oracle class create oracle dcx object to load data from oracle database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'oracle')

class MsSQL(DBCX):
    def __init__(self,config):
        """
        MsSQL class create mssql dcx object to load data from mssql database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
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


# class MariaDB():
#     def __init__(self,config):
#         """
#         MariaDB class create dcx mariadb object, through which you can able to read, write, download data from MariaDB.

#         Args:
#             config (dict): Automatically loaded from the config file (yaml)
#         """
#         self._config = config
#         if 'DATABASE' in config:
#             if config['DATABASE']:
#                 self._dbname_in_config = True
#                 self._mdb_conn = mariadb.connect(
#                     user=config['USERNAME'],
#                     password=config['PASSWORD'],
#                     host=config['HOST'],
#                     port=config['PORT'],
#                     database=config['DATABASE']
#                 )

#     def read_as_dataframe(self,query: str,database: str=None, return_type='pandas'):
#         """_summary_

#         Args:
#             query (str): select query
#             database (str, optional): database name, if None, it take it from config. Defaults to None.
#             return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'. Defaults to 'pandas'.

#         Returns:
#             DataFrame: Depends on the return_type parameter.
#         """
#         if self._dbname_in_config:
#             cur = self._mdb_conn.cursor()
#         elif database:
#             self._mdb_conn = mariadb.connect(
#                     user=self._config['USERNAME'],
#                     password=self._config['PASSWORD'],
#                     host=self._config['HOST'],
#                     port=self._config['PORT'],
#                     database=self._config['DATABASE']
#                 )
#             cur = self._mdb_conn.cursor()
#         cur.execute(query)
#         records = [row for row in cur]
#         cur.close()
#         return pd.DataFrame(records)
    
#     def download_as_file(self,query: str, filename: str, database: str = None):
#         """
#         Takes query as argument and download the data as file

#         Args:
#             query (str): select query
#             filename (str): filename to save the file
#             database (str, optional): database name, if None, it take it from config. Defaults to None.
#         """
#         df = self.read_as_dataframe(query=query, database=database, return_type='pandas')
#         _df_to_file_writer(df, filename=filename)
#         print('File saved to the path:', filename)
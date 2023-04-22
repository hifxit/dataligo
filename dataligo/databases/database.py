import connectorx as cx
import pandas as pd
#import mariadb
from ..exceptions import ParamsMissingException, UnSupportedDataFrameException
from ..datawarehouses.utils import _df_to_file_writer
import os
from sqlalchemy import create_engine
from ..utils import which_dataframe

class DBCX():
    def __init__(self,config,db_type):
        """
        DBCX is the parent class for most of the Database, which use ConnectorX to read and download the data from the database.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
            db_type (str): database type (eg: mysql, postgresql, etc). It is passed by the child class
        """
        self.db_type = db_type
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

    def write_dataframe(self, df,  table_name: str, database: str = None, if_exists: str = 'append',index=False):
        """
        Takes dataframe, table name as arguments and write the dataframe to database

        Args:
            df (DataFrame): Dataframe which need to be loaded
            table_name (str): table name
            database (str, optional): database name. Defaults to None.
            if_exists (str, optional): operation to do if the table exists. Defaults to 'append'.
            index (bool, optional): Write DataFrame index as a column. Defaults to False.
        """
        if self._dbname_in_config:
            engine = create_engine(self._conn_str)    
        elif database:
            engine = create_engine(f"{self._conn_str}/{database}")
        else:
            raise ParamsMissingException(f"database parameter missing. Either add it in config file or pass it as an argument.")

        if which_dataframe(df)=='pandas':
            df.to_sql(table_name,engine,if_exists=if_exists,index=index)
        elif which_dataframe(df)=='polars':
            df.to_pandas().to_sql(table_name,engine,if_exists=if_exists,index=index)
        else:
            raise UnSupportedDataFrameException(f"Unsupported Dataframe: {which_dataframe(df)}")
        print("Dataframe saved to the table:", f"{table_name}")


class Postgres(DBCX):
    def __init__(self,config):
        """
        Postgres class create postgresql ligo object to load data from postgresql database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'postgresql')

class MySQL(DBCX):
    def __init__(self,config):
        """
        MySQL class create mysql ligo object to load data from mysql database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'mysql')

class Oracle(DBCX):
    def __init__(self,config):
        """
        Oracle class create oracle ligo object to load data from oracle database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'oracle')

class MsSQL(DBCX):
    def __init__(self,config):
        """
        MsSQL class create mssql ligo object to load data from mssql database

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'mssql')
    
    def write_dataframe(self, df, table_name: str, database: str = None, if_exists: str = 'append', index=False):
        """
        Takes dataframe, table name as arguments and write the dataframe to MsSQL

        Args:
            df (DataFrame): Dataframe which need to be loaded
            table_name (str): table name
            database (str, optional): database name. Defaults to None.
            if_exists (str, optional): operation to do if the table exists. Defaults to 'append'.
            index (bool, optional): Write DataFrame index as a column. Defaults to False.
        """
        conn_str = self.conn_str.replace('mssql','mssql+pymssql')
        if self._dbname_in_config:
            engine = create_engine(conn_str)    
        elif database:
            engine = create_engine(f"{conn_str}/{database}")
        else:
            raise ParamsMissingException(f"database parameter missing. Either add it in config file or pass it as an argument.")
        
        if which_dataframe(df)=='pandas':
            df.to_sql(table_name,engine,if_exists=if_exists,index=index)
        elif which_dataframe(df)=='polars':
            df.to_pandas().to_sql(table_name,engine,if_exists=if_exists,index=index)
        else:
            raise UnSupportedDataFrameException(f"Unsupported Dataframe: {which_dataframe(df)}")
        print("Dataframe saved to the table:", f"{table_name}")

class Sqlite():
    def __init__(self,config):
        """
        Sqlite class create sqlite ligo object to load data from sqlite database

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
        
    def write_dataframe(self,df, table_name: str, db_path: str = None, if_exists: str = 'append',index=False):
        """
        Takes dataframe, table name as arguments and write the dataframe to SQLite

        Args:
            df (DataFrame): Dataframe which need to be loaded
            table_name (str): table name
            db_path (str, optional): database path. Defaults to None.
            if_exists (str, optional): operation to do if the table exists. Defaults to 'append'.
            index (bool, optional): Write DataFrame index as a column. Defaults to False.
        """
        if db_path:
            abs_db_path = os.path.abspath(db_path)
        else:
            db_path = self._sqlite_conn.split('//')[-1]
            abs_db_path = os.path.abspath(db_path)
        conn_str = 'sqlite:///'+abs_db_path
        engine = create_engine(conn_str)
        if which_dataframe(df)=='pandas':
            df.to_sql(table_name,engine,if_exists=if_exists,index=index)
        elif which_dataframe(df)=='polars':
            df.to_pandas().to_sql(table_name,engine,if_exists=if_exists,index=index)
        else:
            raise UnSupportedDataFrameException(f"Unsupported Dataframe: {which_dataframe(df)}")
        print("Dataframe saved to the table:", f"{table_name}")

class MariaDB(DBCX):
    def __init__(self,config):
        """
        MariaDB class create mariadb ligo object to load data from mariadb database, through mysql protocol 

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'mysql')

# class MariaDB():
#     def __init__(self,config):
#         """
#         MariaDB class create ligo mariadb object, through which you can able to read, write, download data from MariaDB.

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
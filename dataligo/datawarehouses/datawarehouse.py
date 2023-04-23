import connectorx as cx
from .utils import _df_to_file_writer, _snowflake_connector, _snowflake_executer
from ..databases.database import DBCX
from ..exceptions import ParamsMissingException, UnSupportedDataFrameException
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
from google.oauth2 import service_account
from ..utils import which_dataframe

class SnowFlake():
    def __init__(self, config):
        """
        SnowFlake class create the ligo snowflake object, through which you can able to read, write, download data from SnowFlake.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._config = config
        
    def read_as_dataframe(self,query: str,database: str = None,schema: str = None,protocol: str ='https',return_type: str ='pandas'):
        """
        Takes query as arguments and return dataframe

        Args:
            query (str): select query
            database (str, optional): database name, if None, it take it from config. Defaults to None.
            schema (str, optional): schema name, if None, it take it from config. Defaults to None.
            protocol (str, optional): protocol Defaults to 'https'.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'. Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        sf_conn = _snowflake_connector(self._config, database=database, schema=schema, protocol=protocol)
        df = _snowflake_executer(sf_conn, query, return_type=return_type)
        sf_conn.close()
        return df
    
    def download_as_file(self, query: str, filename: str, database: str = None, schema: str = None, protocol: str = 'https') -> None:
        """
        Takes query, filename as arguments and download the data as file

        Args:
            query (str): select query
            filename (str): filename to save the file
            database (str, optional): database name, if None, it take it from config. Defaults to None.
            schema (str, optional): schema name, if None, it take it from config. Defaults to None.
        """
        sf_conn = _snowflake_connector(self._config, database=database, schema=schema, protocol=protocol)
        df = _snowflake_executer(sf_conn, query, return_type='pandas')
        _df_to_file_writer(df,filename)
        print('File saved to the path:', filename)

    # source: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#write_pandas
    def write_dataframe(self,df,table_name: str, database: str = None, schema: str = None, protocol: str = 'https'):
        """
        Takes dataframe, table name as arguments and write the dataframe to SnowFlake

        Args:
            df (Dataframe): Dataframe which need to be loaded
            table_name (str): table name
            database (str, optional): database name. Defaults to None.
            schema (str, optional): schema name. Defaults to None.
            protocol (str, optional): protocol used. Defaults to 'https'.
        """
        sf_conn = _snowflake_connector(self._config, database=database, schema=schema, protocol=protocol)
        if which_dataframe(df)=='pandas':
            success, nchunks, nrows, _ = write_pandas(sf_conn, df, table_name)
        elif which_dataframe(df)=='polars':
            success, nchunks, nrows, _ = write_pandas(sf_conn, df.to_pandas(), table_name)
        else:
            raise UnSupportedDataFrameException(f"Unsupported Dataframe: {which_dataframe(df)}")
        print("Dataframe saved to the snowflake table:", f"{table_name}")
        

class BigQuery():
    def __init__(self,config):
        """
        BigQuery class create the ligo biqquery object, through which you can able to read, write, download data from Google's BigQuery 

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._config = config
        self._bq_conn = 'bigquery://' + config['GOOGLE_APPLICATION_CREDENTIALS_PATH']

    def read_as_dataframe(self, query: str,return_type: str ='pandas'):
        """
        Takes query as the arguments and return the dataframe

        Args:
            query (str): select query
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'. Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        return cx.read_sql(self._bq_conn, query,return_type=return_type)
    
    def download_as_file(self, query: str, filename: str) -> None:
        """
        Takes query, filename as arguments and download the data as file

        Args:
            query (str): select query
            filename (str): filename to save the file
        """
        df = cx.read_sql(self._bq_conn, query,return_type='pandas')
        _df_to_file_writer(df,filename)
        print('File saved to the path:', filename)

    def write_dataframe(self, df, table_name: str, project_id: str, if_exists: str = 'append') -> None:
        """
        Takes dataframe, table name, project id as arguments and write the dataframe to BigQuery

        Args:
            df (DataFrame): Dataframe which need to be loaded
            table_name (str): table name
            project_id (str): project id
            if_exists (str, optional): operation to do if the table exists. Defaults to 'append'.
        """
        credentials = service_account.Credentials.from_service_account_file(self._config['GOOGLE_APPLICATION_CREDENTIALS_PATH'])
        if which_dataframe(df)=='pandas': 
            df.to_gbq(destination_table=table_name, project_id=project_id, if_exists=if_exists, credentials=credentials)
        elif which_dataframe(df)=='polars':
            df.to_pandas().to_gbq(destination_table=table_name, project_id=project_id, if_exists=if_exists, credentials=credentials)
        else:
            raise UnSupportedDataFrameException(f"Unsupported Dataframe: {which_dataframe(df)}")
        print("Dataframe saved to the table:", f"{table_name}")

    
class Redshift(DBCX):
    def __init__(self, config) -> None:
        """
        Redshift class create the ligo redshift object, through which you can able to read, write, download data from Redshift.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'redshift')
    
    def write_dataframe(self, df,  table_name: str, database: str = None, if_exists: str = 'append',index=False):
        """
        Takes dataframe, table name as arguments and write the dataframe to Redshift

        Args:
            df (DataFrame): Dataframe which need to be loaded
            table_name (str): table name
            database (str, optional): database name. Defaults to None.
            if_exists (str, optional): operation to do if the table exists. Defaults to 'append'.
            index (bool, optional): Write DataFrame index as a column. Defaults to False.
        """
        conn_str = self._conn_str.replace('redshift','postgresql')
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
        
class StarRocks():
    """
    StarRocks class create the ligo starrocks object, through which you can able to read, write, download data from StarRocks.

    Args:
        config (dict): Automatically loaded from the config file (yaml)
    """
    def __init__(self, config) -> None:
        self._config = config
        self._sr_conn = mysql.connector.connect(host = config['HOST'],port = config['PORT'], user = config['USERNAME'], password=config['PASSWORD']) 
        if 'DATABASE' in config:
            if config['DATABASE']:
                self._dbname_in_config = True
                self._sr_conn = mysql.connector.connect(
                                        host = config['HOST'],
                                        port = config['PORT'],
                                        user = config['USERNAME'], 
                                        password=config['PASSWORD'],
                                        database=config['DATABASE']) 
                
    def read_as_dataframe(self, query: str, database: str = None, return_type: str ='pandas'):
        """
        Takes query as argument and return a dataframe

        Args:
            query (str): select query
            database (str, optional): database name, if None, it take it from config. Defaults to None.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'. Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        if self._dbname_in_config:
            cur = self._sr_conn.cursor()
        elif database:
            self._sr_conn = mysql.connector.connect(
                                        host = self._config['HOST'],
                                        port = self._config['PORT'],
                                        user = self._config['USERNAME'], 
                                        password=self._config['PASSWORD'],
                                        database=self._config['DATABASE'])
        else:
            raise ParamsMissingException(f"database parameter missing. Either add it in config file or pass it as an argument.")
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        if return_type=='pandas':
            return pd.DataFrame(cur.fetchall(), columns=columns)
        elif return_type=='polars':
            return pl.from_records(cur.fetchall(), schema=columns)
        
    
    def download_as_file(self, query: str, filename: str, database: str = None) -> None:
        """
        Takes query, filename as arguments and download the data as file

        Args:
            query (str): select query
            filename (str): filename to save the file
            database (str, optional): database name, if None, it take it from config. Defaults to None.
        """
        df = self.read_as_dataframe(query=query,database=database)
        _df_to_file_writer(df,filename=filename)
        print('File saved to the path:', filename)

    def write_dataframe(self, df,  table_name: str, database: str = None, if_exists: str = 'append',index=False):
        """
        Takes dataframe, table name as arguments and write the dataframe to StarRocks

        Args:
            df (DataFrame): Dataframe which need to be loaded
            table_name (str): table name
            database (str, optional): database name. Defaults to None.
            if_exists (str, optional): operation to do if the table exists. Defaults to 'append'.
            index (bool, optional): Write DataFrame index as a column. Defaults to False.
        """
        config = self._config
        conn_str = f"mysql://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
        if self._dbname_in_config:
            conn_str = f"{conn_str}/{config['DATABASE']}"
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
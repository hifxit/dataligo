import connectorx as cx
from .utils import _df_to_file_writer, _snowflake_connector, _snowflake_executer
from ..databases.database import DBCX
from ..exceptions import ParamsMissingException
import mysql.connector
import pandas as pd

class SnowFlake():
    def __init__(self, config):
        """
        SnowFlake class create the dcx snowflake object, through which you can able to read, write, download data from SnowFlake.

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

class BigQuery():
    def __init__(self,config):
        """
        BigQuery class create the dcx biqquery object, through which you can able to read, write, download data from Google's BigQuery 

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
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

    
class Redshift(DBCX):
    def __init__(self, config) -> None:
        """
        Redshift class create the dcx redshift object, through which you can able to read, write, download data from Redshift.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        super().__init__(config,'redshift')
        
class StarRocks():
    """
    StarRocks class create the dcx starrocks object, through which you can able to read, write, download data from StarRocks.

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
        return pd.DataFrame(cur.fetchall(),columns=columns)
    
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
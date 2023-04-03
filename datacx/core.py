import yaml
from .datalakes.datalake import S3, GCS, AzureBlob
from .datawarehouses.datawarehouse import BigQuery, SnowFlake, Redshift, StarRocks
from .databases.database import Postgres, MySQL, Oracle, MsSQL, Sqlite
from .nosql.nosql import ElasticSearch, MongoDB
from .exceptions import ConfigMissingException, UnSupportedDataSourceException

DATA_SOURCES = {
    's3': S3, # AWS S3
    'gcs': GCS, # Google Cloud Storage
    'azureblob': AzureBlob, # Azure Blob Storage
    'bigquery': BigQuery, # Google BigQuery
    'snowflake': SnowFlake, # SnowFlake
    'redshift': Redshift, # AWS Redshift
    'starrocks': StarRocks, # StarRocks
    'postgresql': Postgres, # PostgreSQL
    'mysql': MySQL, # MySQL
    'oracle': Oracle, # Oracle
    'mssql': MsSQL, # MsSQL, SQLServer
#    'mariadb': MariaDB, # MariaDB
    'sqlite': Sqlite, # Sqlite
    'elasticsearch': ElasticSearch, # ElasticSearch
    'mongodb': MongoDB, # MongoDB

}

DATA_SOURCE_GROUP = {
    'datalakes': ['s3','gcs','azureblob'],
    'datawarehouses': ['snowflake','redshift','bigquery','starrocks','synapse'],
    'databases': ['postgresql','mssql','mysql','oracle','mariadb','sqlite'],
    'nosql': ['mongodb','elasticsearch','dynamodb']
}

class DataCX():
    def __init__(self,config_path: str=None, name: str = None) -> None:
        """
        DataCX class create the dcx object which act as the entrypoint for all the data sources.

        Args:
            config_path (str, optional): path of the config file (yaml). Defaults to None.
            name (str, optional): name of the dcx object. Useful if using multiple dcx object. Defaults to None.
        """
        self.config_path = config_path
        self.name = name
        if config_path is not None:
            self.set_config(self.config_path)

    def set_config(self,config_path: str) -> None:
        """
        Takes config path as arguments and setup the configuration

        Args:
            config_path (str): path of the config file (yaml).
        """
        self.config_path = config_path
        with open(self.config_path,'r') as config_file:
            self._config = yaml.safe_load(config_file)

    def get_supported_data_sources_list(self) -> None:
        """
        Returns the list of supported data sources

        Returns:
            list: list of supported data sources
        """
        return list(DATA_SOURCES.keys())

    def connect(self,data_source: str):
        """
        Takes data source name as input and return the dcx data source object

        Args:
            data_source (str): data source name

        Returns:
            object: dcx data source object
        """
        data_source = data_source.lower()
        supported_data_sources = self.get_supported_data_sources_list()
        if data_source not in supported_data_sources:
            raise UnSupportedDataSourceException("Mentioned Data Source not supported. Supported Data Sources are",supported_data_sources)
        if self.config_path:
            ds_group = self._config_mapper(data_source)
            ds_config = self._config[ds_group][data_source]
            return DATA_SOURCES[data_source](ds_config)
        else:
            raise ConfigMissingException("Config file missing. Add the config file path using set_config method.")

    # helper function  
    def _config_mapper(self,data_source) -> str:
        return [key for key, value in DATA_SOURCE_GROUP.items() if data_source in value][0]


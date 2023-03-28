import yaml
from .datalakes.datalake import S3, GCS, AzureBlob
from .datawarehouses.datawarehouse import BigQuery, SnowFlake, Redshift
from .databases.database import Postgres, MySQL, Oracle, MariaDB, MsSQL, Sqlite
from .nosql.nosql import ElasticSearch, MongoDB
from .exceptions import ConfigMissingException

_data_sources = {
    's3': S3, # AWS S3
    'gcs': GCS, # Google Cloud Storage
    'azureblob': AzureBlob, # Azure Blob Storage
    'bigquery': BigQuery, # Google BigQuery
    'snowflake': SnowFlake, # SnowFlake
    'redshift': Redshift, # AWS Redshift
    'postgresql': Postgres, # PostgreSQL
    'mysql': MySQL, # MySQL
    'oracle': Oracle, # Oracle
    'mssql': MsSQL, # MsSQL, SQLServer
    'mariadb': MariaDB, # MariaDB
    'sqlite': Sqlite, # Sqlite
    'elasticsearch': ElasticSearch, # ElasticSearch
    'mongodb': MongoDB, # MongoDB

}

_data_source_group = {
    'datalakes': ['s3','gcs','azureblob'],
    'datawarehouses': ['snowflake','redshift','bigquery','synapse'],
    'databases': ['postgresql','mssql','mysql','oracle','mariadb','sqlite'],
    'nosql': ['mongodb','elasticsearch','dynamodb']
}

class datacx():
    def __init__(self,config_path: str=None) -> None:
        self.config_path = config_path
        if config_path is not None:
            self.set_config(self.config_path)

    def set_config(self,config_path: str) -> None:
        self.config_path = config_path
        with open(self.config_path,'r') as config_file:
            self._config = yaml.safe_load(config_file)

    def get_supported_data_sources_list(self) -> None:
        print(list(_data_sources.keys()))

    def connect(self,data_source):
        if self.config_path:
            ds_group = self._config_mapper(data_source)
            ds_config = self._config[ds_group][data_source]
            return _data_sources[data_source](ds_config)
        else:
            raise ConfigMissingException("Config file missing. Add the config file path using set_config method.")
        
    def _config_mapper(self,data_source) -> str:
        return [key for key, value in _data_source_group.items() if data_source in value][0]


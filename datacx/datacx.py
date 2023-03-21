import yaml
from .datalakes import s3, gcs, abs
from .datawarehouses import BigQuery, SnowFlake, Redshift
from .databases import Postgres
from .nosql import ElasticSearch

_data_sources = {
    's3': s3, # AWS S3
    'gcs': gcs, # Google Cloud Storage
    'abs': abs, # Azure Blob Storage
    'bigquery': BigQuery, # Google BigQuery
    'snowflake': SnowFlake, # SnowFlake
    'redshift': Redshift, # AWS Redshift
    'postgresql': Postgres, # PostgreSQL
    'elasticsearch': ElasticSearch, # ElasticSearch

}

_data_source_group = {
    'datalakes': ['s3','gcs','abs'],
    'datawarehouses': ['snowflake','redshift','bigquery','synapse'],
    'sql': ['postgresql','sqlserver','mysql','mariadb','sqlite'],
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

    def connect(self,data_source) -> s3:
        ds_group = self._config_mapper(data_source)
        ds_config = self._config[ds_group][data_source]
        return _data_sources[data_source](ds_config)
    
    def _config_mapper(self,data_source) -> str:
        return [key for key, value in _data_source_group.items() if data_source in value][0]


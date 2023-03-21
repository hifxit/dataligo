import connectorx as cx
from snowflake import connector

class SnowFlake():
    def __init__(self, config):
        self._config = config
        
    def read_as_dataframe(self,query,database,schema,protocol='https',return_type='pandas'):
        sf_conn = connector.connect(
            host = self._config['HOST'],
            user = self._config['USERNAME'],
            password=self._config['PASSWORD'],
            account=self._config['ACCOUNT_NAME'],
            database=database,
            schema=schema,
            protocol=protocol
          )
        cur = sf_conn.cursor()
        cur.execute(query)
        data = cur.fetch_pandas_all()
        return data

class BigQuery():
    def __init__(self,config):
        self._bq_conn = 'bigquery://' + config['GOOGLE_APPLICATION_CREDENTIALS_PATH']

    def read_as_dataframe(self,query,return_type='pandas'):
        return cx.read_sql(self._bq_conn, query,return_type=return_type)
    
class Redshift():
    def __init__(self,config):
        self._pg_conn_str = f"redshift://{config['USERNAME']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}"
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
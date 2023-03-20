import connectorx as cx
from snowflake import connector

class SnowFlake():
    def __init__(self, config):
        self._config = config
        
    def read_as_dataframe(self,query,database,schema,protocol='https',return_type='pandas'):
        print(self._config)
        sf_conn = connector.connect(
            host = self._config['HOST'],
            user = self._config['USERNAME'],
            password=self._config['PASSWORD'],
            account=self._config['ACCOUNT_NAME'],
            database=database,
            schema=schema,
            protocol=protocol
          )
        print(query)
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
    pass
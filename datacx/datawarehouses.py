import connectorx as cx

class BigQuery():
    def __init__(self,config):
        self._bq_conn = 'bigquery://' + config['GOOGLE_APPLICATION_CREDENTIALS_PATH']

    def read_as_dataframe(self,query,return_type='pandas'):
        return cx.read_sql(self._bq_conn, query,return_type=return_type)
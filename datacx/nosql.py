from elasticsearch import Elasticsearch
import pandas as pd

class ElasticSearch():
    def __init__(self,config):
        self._es = ElasticSearch([config['HOST']],port=config['PORT'])

    def read_as_dict(self,query,index):
        response = self._es.search(
            index = index,
            body = query
            )
        return response['hits']['hits']
    
    def read_as_dataframe(self,query,index):
        return pd.DataFrame(self.read_as_dict(query,index))

class MongoDB():
    pass

class DynamoDB():
    pass
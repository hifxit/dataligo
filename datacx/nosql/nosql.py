from elasticsearch import Elasticsearch
from pymongo import MongoClient
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
    def __init__(self, config) -> None:
        self._mdb = MongoClient(config['CONN_STRING'])

    def read_as_dataframe(self,database,collection,filter_query: dict=None):
        if filter_query is None:
            return pd.DataFrame(list(self._mdb[database][collection].find()))
        else:
            return pd.DataFrame(list(self._mdb[database][collection].find(filter_query)))


class DynamoDB():
    pass
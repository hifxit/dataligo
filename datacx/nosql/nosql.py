from elasticsearch import Elasticsearch
from pymongo import MongoClient
import pandas as pd

class ElasticSearch():
    def __init__(self,config):
        """
        ElasticSearch class create the dcx elasticsearch object, through which you can able to read, write, download data from ElasticSearch.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        if 'API_KEY' in config:
            if config['API_KEY']:
                self._es = ElasticSearch([config['HOST']],api_key=config['API_KEY'])
        else:
            self._es = ElasticSearch([config['HOST']])
    
    def read_as_dataframe(self,query: str,index: str,return_type='pandas'):
        """
        Takes query and index as arguments and return the dataframe

        Args:
            query (str): es query
            index (str): es index

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        response = self._es.search(
            index = index,
            body = query
            )
        records = [i['_source'] for i in response['hits']['hits']]
        return pd.DataFrame(records)

class MongoDB():
    def __init__(self, config) -> None:
        """
        MongoDB class create the dcx mongodb object, through which you can able to read, write, download data from MongoDB.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._mdb = MongoClient(config['CONN_STRING'])

    def read_as_dataframe(self,database: str,collection: str,filter_query: dict=None,return_type='pandas'):
        """
        Takes database, collections as arguments and return the dataframe

        Args:
            database (str): database name
            collection (str): collection name
            filter_query (dict, optional): filter query. Defaults to None.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        if filter_query is None:
            return pd.DataFrame(list(self._mdb[database][collection].find()))
        else:
            return pd.DataFrame(list(self._mdb[database][collection].find(filter_query)))
        
    def write_dataframe(self, df, database: str, collection: str):
        """
        Takes DataFrame, database name, collection name as arguments and write the dataframe to MongoDB.

        Args:
            df (DataFrame): Dataframe which need to be inserted to mongodb
            database (str): database name
            collection (str): collection name
        """
        records = df.to_dict('records')
        self._mdb[database][collections].insert_many(records)
        print("Dataframe saved to the collections:", f"{collections}")


class DynamoDB():
    pass
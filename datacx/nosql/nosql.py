from elasticsearch import Elasticsearch
from pymongo import MongoClient
import pandas as pd
from elasticsearch.helpers import bulk
from dynamo_pandas import put_df, get_df
from typing import List, Dict
from sqlalchemy import create_engine

class ElasticSearch():
    def __init__(self,config):
        """
        ElasticSearch class create the dcx elasticsearch object, through which you can able to read, write, download data from ElasticSearch.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        if 'USERNAME' in config and 'PASSWORD' in config:
            if config['USERNAME'] and config['PASSWORD']:
                self._es = Elasticsearch([config['HOST']],basic_auth=(config['USERNAME'],config['PASSWORD']))
        elif 'API_KEY' in config:
            if config['API_KEY']:
                self._es = Elasticsearch([config['HOST']],api_key=config['API_KEY'])
        else:
            self._es = Elasticsearch([config['HOST']])
    
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

    def write_dataframe(self, df, index: str):
        """
        Takes DataFrame, index name as arguments and write the dataframe to ElasticSearch.
        Args:
            df (DataFrame): Dataframe which need to be inserted to es
            index (str): index name
        """
        records = df.to_dict('records')
        actions = [
            {
                "_index": index,
                "_source": doc
            }
            for doc in records
        ]
        # Perform the bulk insert operation
        bulk(self._es, actions)
        print("Dataframe saved to the es index:", f"{index}")

        
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
        self._mdb[database][collection].insert_many(records)
        print("Dataframe saved to the collections:", f"{collection}")


class DynamoDB():
    def __init__(self, config) -> None:
        self._ddb = {'aws_access_key_id':config['AWS_ACCESS_KEY_ID'],
                     'aws_secret_access_key':config['AWS_SECRET_ACCESS_KEY']}

    def read_as_dataframe(self, table: str, keys=None,attributes=None, dtype=None,return_type='pandas'):
        return get_df(table, keys=keys, attributes=attributes, dtype=dtype, boto3_kwargs=self._ddb)
    
    def write_dataframe(self, df, table: str):
        put_df(df,table=table,boto3_kwargs=self._ddb)
        print("Dataframe records updated to the DynamoDB table:", table)

# source: https://www.cdata.com/kb/tech/redis-python-pandas.rst
class Redis():
    def __init__(self, config) -> None:
        self._redis_engine = create_engine(f"redis:///?Server={config['HOST']}&;Port={config['PORT']}&Password={config['PASSWORD']}")

    def read_as_dataframe(self, query: str, return_type='pandas'):
        return pd.read_sql(query, self._redis_engine)
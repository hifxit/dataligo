<div align="center">

# DataLigo

<div align="left">

This library helps to read and write data from most of the data sources. It accelerate the ML and ETL process without worrying about the multiple data connectors.

## Installation
```bash
pip install -U dataligo
```
**Install from sources**

Alternatively, you can also clone the latest version from the [repository](https://github.com/VinishUchiha/dataligo) and install it directly from the source code:

```bash
pip install -e .
```

## Quick tour
```python
>>> from dataligo import Ligo
>>> from transformers import pipeline

>>> ligo = Ligo('./ligo_config.yaml') # Check the sample_ligo_config.yaml for reference
>>> print(ligo.get_supported_data_sources_list())
['s3', 'gcs', 'azureblob', 'bigquery', 'snowflake', 'redshift', 'starrocks', 'postgresql', 'mysql', 'oracle', 'mssql', 'mariadb', 'sqlite', 'elasticsearch', 'mongodb']

>>> mongodb = ligo.connect('mongodb')
>>> df = mongodb.read_as_dataframe(database='reviewdb',collection='reviews')
>>> df.head()
        _id	                        Review
0	64272bb06a14f52787e0a09e	good and interesting
1	64272bb06a14f52787e0a09f	This class is very helpful to me. Currently, I...
2	64272bb06a14f52787e0a0a0	like!Prof and TAs are helpful and the discussi...
3	64272bb06a14f52787e0a0a1	Easy to follow and includes a lot basic and im...
4	64272bb06a14f52787e0a0a2	Really nice teacher!I could got the point eazl...

>>> classifier = pipeline("sentiment-analysis")
>>> reviews = df.Review.tolist()
>>> results = classifier(reviews,truncation=True)
>>> for result in results:
>>>     print(f"label: {result['label']}, with score: {round(result['score'], 4)}")
label: POSITIVE, with score: 0.9999
label: POSITIVE, with score: 0.9997
label: POSITIVE, with score: 0.9999
label: POSITIVE, with score: 0.999
label: POSITIVE, with score: 0.9967

>>> df['predicted_label'] = [result['label'] for result in results]
>>> df['predicted_score'] = [round(result['score'], 4) for result in results]

# Write the results to the MongoDB
>>> mongodb.write_dataframe(df,'reviewdb','review_sentiments')
```
## Supported Connectors

        
 |Data Sources| Type | pandas | polars | dask |
|------------|------| ----  | -----| ----- |
|S3|datalake| <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|GCS|datalake| <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|Azure Blob Stoarge| datalake| <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|Snowflake| datawarehouse | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|BigQuery| datawarehouse | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|StarRocks| datawarehouse | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|Redshift| datawarehouse | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|PostgreSQL| database | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|MySQL| database | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|MariaDB| database | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|MsSQL| database | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|Oracle| database | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|SQLite| database | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[x] read</li><li>[ ] write</li></ul> | <ul><li>[x] read</li><li>[ ] write</li></ul> |
|MongoDB| nosql | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|ElasticSearch| nosql | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|DynamoDB| nosql | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |
|Redis| nosql | <ul><li>[x] read</li><li>[x] write</li></ul>   | <ul><li>[ ] read</li><li>[ ] write</li></ul> | <ul><li>[ ] read</li><li>[ ] write</li></ul> |


## Acknowledgement

Some functionalities of DataLigo are inspired by the following packages.

- [ConnectorX](https://github.com/sfu-db/connector-x)
  
  DataLigo used Connectorx to read data from most of the RDBMS databases to utilize the performance benefits and inspired the return_type parameter from it
  

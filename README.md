<div align="center">

<img src="docs/images/LOGO.png" alt="drawing" width="200"/>

A Data Connector for all the data sources

<div align="left">

This library helps to read and write data from most of the data sources. It accelerate the ML development process without worrying about the multiple data connectors.

## Installation
```bash
pip install datacx
```
**Install from sources**

Alternatively, you can also clone the latest version from the [repository](https://github.com/VinishUchiha/datacx) and install it directly from the source code:

```bash
pip install -e .
```

## Quick tour
```python
>>> from datacx import DataCX
>>> from transformers import pipeline

>>> dcx = DataCX('./dcx_config.yaml') # Check the sample_dcx_config.yaml for reference
>>> print(dcx.get_supported_data_sources_list())
['s3', 'gcs', 'azureblob', 'bigquery', 'snowflake', 'redshift', 'starrocks', 'postgresql', 'mysql', 'oracle', 'mssql', 'mariadb', 'sqlite', 'elasticsearch', 'mongodb']

>>> mongodb = dcx.connect('mongodb')
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
  
|Data Sources| Type | Read | Write |
|------------|------| ----  | -----|
|S3|datalake| &#9745;   | &#9745; |
|GCS|datalake| &#9745;   | &#9745; |
|Azure Blob Stoarge| datalake| &#9745;   | &#9745; |
|Snowflake| datawarehouse | &#9745;   | &#9744; |
|BigQuery| datawarehouse | &#9745;   | &#9744; |
|StarRocks| datawarehouse | &#9745;   | &#9744; |
|Redshift| datawarehouse | &#9745;   | &#9744; |
|PostgreSQL| database | &#9745;   | &#9744; |
|MySQL| database | &#9745;   | &#9744; |
|MsSQL| database | &#9745;   | &#9744; |
|MariaDB| database | &#9745;   | &#9744; |
|Oracle| database | &#9745;   | &#9744; |
|SQLite| database | &#9745;   | &#9744; |
|MongoDB| nosql | &#9745;   | &#9745; |
|ElasticSearch| nosql | &#9745;   | &#9744; |

## Acknowledgement

Some functionalities of DataCX are inspired by the following packages.

- [ConnectorX](https://github.com/sfu-db/connector-x)
  
  DataCX used Connectorx to read data from most of the RDBMS databases to utilize the performance benefits and inspired the return_type parameter from it
  
- [GeneratorREX](https://generatorrex.fandom.com/wiki/Generator_Rex_Wiki)
  
  DataCX logo inspired by the American animated science fiction television series and created by my graphic designer friend [Belgin David](https://www.linkedin.com/in/belgin-david-4b699a1b8)

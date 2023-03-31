<div align="center">

<img src="docs/images/LOGO.png" alt="drawing" width="200"/>

A Data Connector for all the data sources

<div align="left">

## Installation
```bash
pip install datacx
```

## Quick tour
```python
>>> from datacx import DataCX
>>> from transformers import pipeline

>>> dcx = DataCX('./dcx_config.yaml') # Check the sample_dcx_config.yaml for reference
>>> mongodb = dcx.connect('mongodb')
>>> df = mongodb.read_as_dataframe(database='reviewdb',collection='reviews')

>>> classifier = pipeline("sentiment-analysis")
>>> reviews = df.Review.tolist()
>>> results = classifier(reviews,truncation=True)
>>> df['predicted_label'] = [result['label'] for result in results]
>>> df['predicted_score'] = [round(result['score'], 4) for result in results]

>>> mongodb.write_dataframe(df,'reviewdb','review_sentiments')
```

## Acknowledgement

Some functionalities of DataCX are inspired by the following packages.

- [ConnectorX](https://github.com/sfu-db/connector-x)

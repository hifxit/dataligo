import pandas as pd
from pathlib import Path
from snowflake import connector
from ..exceptions import ExtensionNotSupportException

def _snowflake_connector(config, database, schema, protocol):
    if database and schema:
        sf_conn = connector.connect(
            host = config['HOST'],
            user = config['USERNAME'],
            password= config['PASSWORD'],
            account= config['ACCOUNT_NAME'],
            database= database,
            schema= schema,
            protocol= protocol
        )
    else:
        sf_conn = connector.connect(
            host = config['HOST'],
            user = config['USERNAME'],
            password= config['PASSWORD'],
            account= config['ACCOUNT_NAME'],
            database= config['DATABASE'],
            schema= config['SCHEMA'],
            protocol= protocol
        )
    return sf_conn

def _snowflake_executer(conn, query, return_type='pandas'):
    cur = conn.cursor()
    cur.execute(query)
    if return_type=='pandas':
        data = cur.fetch_pandas_all()
    return data

def _df_to_file_writer(df,filename: str) -> None:
    suffix = Path(filename).suffix
    if suffix:
        extension = suffix[1:]
    else:
        extension = 'csv'
    if extension=='csv':
        df.to_csv(filename, index=False)
    elif extension=='parquet':
        df.to_parquet(filename)
    elif extension=='json':
        df.to_json(filename)
    elif extension=='xlsx':
        df.to_excel(filename)
    elif extension=='xls':
        df.to_excel(filename)
    elif extension=='feather':
        df.to_feather(filename)
    else:
        raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
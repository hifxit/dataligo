import boto3
from google.cloud import storage
import pandas as pd
from io import BytesIO

_readers = {'csv': pd.read_csv,'parquet': pd.read_parquet}

def _bytes_to_df(body,file_type,reader):
    if file_type=='csv':
        df = reader(body, encoding='utf8')
    else:
        df = reader(body)
    return df

def _multi_file_load(s3,bucket,key,reader,file_type):
    bucket = s3.Bucket(bucket)
    pfx_objs = bucket.objects.filter(Prefix=key[:-1])
    pfx_dfs = []
    for obj in pfx_objs:
        if obj.key.endswith('/'):
            continue
        body = BytesIO(obj.get()['Body'].read())
        df = _bytes_to_df(body,file_type,reader)
        pfx_dfs.append(df)
    return pfx_dfs

class s3():
    def __init__(self,config):
        self._s3 = boto3.resource(
            "s3",
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
        )

    def read_as_pandas_df(self,s3_path=None,file_type='csv'):
        reader = _readers[file_type]
        bucket, key =  s3_path.split('/',3)[2:]
        if key.endswith('*') or key.endswith('/'):
            pfx_dfs = _multi_file_load(self._s3,bucket=bucket,key=key,reader=reader,file_type=file_type)
            df = pd.concat(pfx_dfs).reset_index(drop=True)
            return df
        else:
            obj = self._s3.Object(bucket_name=bucket, key=key)
            body = BytesIO(obj.get()['Body'].read())
            df = _bytes_to_df(body,file_type,reader)
            return df


class gcs():
    def __init__(self,config):
        self._gcs = storage.Client.from_service_account_json(json_credentials_path=config['PRIVATE_KEY_PATH'])

    def read_as_pandas_df(self,gcs_path,file_type='csv'):
        reader = _readers[file_type]
        bucket, file_path = gcs_path.split('/',3)[2:]
        bucket = self._gcs.get_bucket(bucket)
        blob = bucket.blob(file_path)
        data = blob.download_as_string()
        body = BytesIO(data)
        df = _bytes_to_df(body,file_type,reader)
        return df



class abs():
    pass
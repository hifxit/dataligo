import boto3
import pandas as pd
import io

_readers = {'csv': pd.read_csv,'parquet': pd.read_parquet}

def _object_to_df(obj,file_type,reader):
    body = io.BytesIO(obj.get()['Body'].read())
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
        df = _object_to_df(obj,file_type,reader)
        pfx_dfs.append(df)
    return pfx_dfs

class s3():
    def __init__(self,config):
        self._s3 = boto3.resource(
            "s3",
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
        )

    def read_as_pandas_df(self,s3_path=None,bucket=None, key=None,file_type='csv'):
        reader = _readers[file_type]
        if s3_path is not None:
            bucket, key =  s3_path.split('/',3)[2:]
        if key.endswith('*') or key.endswith('/'):
            pfx_dfs = _multi_file_load(self._s3,bucket=bucket,key=key,reader=reader,file_type=file_type)
            df = pd.concat(pfx_dfs).reset_index(drop=True)
            return df
        else:
            obj = self._s3.Object(bucket_name=bucket, key=key)
            df = _object_to_df(obj,file_type,reader)
            return df


class gcs():
    pass

class abs():
    pass
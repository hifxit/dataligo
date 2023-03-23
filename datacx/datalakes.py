import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import BytesIO
from pathlib import Path

_readers = {'.csv': pd.read_csv,'.parquet': pd.read_parquet}

def _bytes_to_df(body,extension,reader):
    if extension=='.csv':
        df = reader(body, encoding='utf8')
    else:
        df = reader(body)
    return df

def _multi_file_load(s3,bucket,key,reader,extension):
    bucket = s3.Bucket(bucket)
    pfx_objs = bucket.objects.filter(Prefix=key[:-1])
    pfx_dfs = []
    for obj in pfx_objs:
        if obj.key.endswith('/'):
            continue
        body = BytesIO(obj.get()['Body'].read())
        df = _bytes_to_df(body,extension,reader)
        pfx_dfs.append(df)
    return pfx_dfs

class s3():
    def __init__(self,config):
        self._s3 = boto3.resource(
            "s3",
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
        )

    def read_as_dataframe(self,s3_path=None,extension='.csv', return_type='pandas'):
        if Path(s3_path).suffix:
            extension = Path(s3_path).suffix
        reader = _readers[extension]
        bucket, key =  s3_path.split('/',3)[2:]
        if key.endswith('*') or key.endswith('/'):
            pfx_dfs = _multi_file_load(self._s3,bucket=bucket,key=key,reader=reader,extension=extension)
            df = pd.concat(pfx_dfs).reset_index(drop=True)
            return df
        else:
            obj = self._s3.Object(bucket_name=bucket, key=key)
            stream = BytesIO(obj.get()['Body'].read())
            df = _bytes_to_df(stream,extension,reader)
            return df
        
    def write_dataframe(self,df,bucket,filename,index=False,sep=',') -> None:
        csv_buf = BytesIO()
        df.to_csv(csv_buf, index=index, sep=sep)
        csv_buf.seek(0)
        s3.Bucket(bucket).put_object(
            Key=filename, Body=csv_buf.getvalue()
        )
        print("Dataframe saved to the s3 path:", f"s3://{bucket}/{filename}")


class gcs():
    def __init__(self,config):
        self._gcs = storage.Client.from_service_account_json(json_credentials_path=config['PRIVATE_KEY_PATH'])

    def read_as_dataframe(self,gcs_path,extension='.csv', return_type='pandas'):
        if Path(gcs_path).suffix:
            extension = Path(gcs_path).suffix
        reader = _readers[extension]
        bucket, file_path = gcs_path.split('/',3)[2:]
        bucket = self._gcs.get_bucket(bucket)
        blob = bucket.blob(file_path)
        data = blob.download_as_string()
        stream = BytesIO(data)
        df = _bytes_to_df(stream,extension,reader)
        return df



class abs():
    def __init__(self,config):
        self._abs = BlobServiceClient(account_url=f"https://{config['ACCOUNT_NAME']}.blob.core.windows.net",
                                        credential=config['ACCOUNT_KEY'])
        
    def read_as_dataframe(self,container_name,blob_name,extension='.csv', return_type='pandas'):
        if Path(blob_name).suffix:
            extension = Path(blob_name).suffix
        reader = _readers[extension]
        container_client = self._abs.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        stream = BytesIO(blob_client.download_blob().readall())
        df = _bytes_to_df(stream,extension,reader)
        return df
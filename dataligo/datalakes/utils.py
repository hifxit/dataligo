from io import BytesIO
from pathlib import Path
import pandas as pd
from ..exceptions import ExtensionNotSupportException
from boto3.s3.transfer import TransferConfig
import os
import threading
import sys
from ..utils import which_dataframe

multipart_config = TransferConfig(multipart_threshold=1024 * 50, 
                        max_concurrency=8,
                        multipart_chunksize=1024 * 10,
                        use_threads=True)

def readers(return_type):
    if return_type=='pandas':
        pd_readers = {'csv': pd.read_csv,'parquet': pd.read_parquet, 'feather': pd.read_feather, 'xlsx': pd.read_excel, 
            'xls': pd.read_excel, 'ods': pd.read_excel, 'json': pd.read_json,'txt': pd.read_csv}
        return pd_readers
    elif return_type=='polars':
        import polars as pl
        pl_readers = {'csv': pl.read_csv,'parquet': pl.read_parquet, 'xlsx': pl.read_excel, 
            'xls': pl.read_excel, 'ods': pl.read_excel, 'json': pl.read_json,'txt': pl.read_csv, 'avro': pl.read_avro}
        return pl_readers

def df_concat(dfs,return_type):
    if return_type=='pandas':     
        df = pd.concat(dfs,ignore_index=True)
        return df
    elif return_type=='polars':
        df = pl.concat(dfs)
        return df

def _multi_file_load(s3,bucket,key,reader,extension,reader_args):
    key = key.strip('/*').strip('*').strip('/')
    bucket = s3.Bucket(bucket)
    pfx_objs = bucket.objects.filter(Prefix=key)
    pfx_dfs = []
    for obj in pfx_objs:
        if obj.key.endswith('/'):
            continue
        body = BytesIO(obj.get()['Body'].read())
        df = reader(body, **reader_args)
        pfx_dfs.append(df)
    return pfx_dfs

def _s3_upload_folder(s3, local_folder_path, bucket, key):
    key = key.rstrip('/')+'/'+Path(local_folder_path).stem
    for root, _ , files in os.walk(local_folder_path):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_folder_path)
            relative_path = relative_path.replace("\\", "/")
            s3_path = os.path.join(key, relative_path)
            s3.Object(bucket, s3_path).upload_file(local_path,
                            Config=multipart_config
                            )

def _s3_download_folder(s3,s3_path, bucket, key, local_path_to_download='.'):
    if s3_path:
        bucket, key =  s3_path.split('/',3)[2:]
    my_bucket = s3.Bucket(bucket)
    folder_to_download = Path(key).stem
    local_path = os.path.join(local_path_to_download, folder_to_download)
    os.makedirs(local_path)
    for obj in my_bucket.objects.filter(Prefix=key):
        key = obj.key
        if key.endswith('/'):
            os.makedirs(os.path.join(local_path,key))
        else:
            s3.Object(bucket, key).download_file(os.path.join(local_path,key),Config=multipart_config)
    print("Folder downloaded to the path:", f"{local_path}")

# source: https://medium.com/analytics-vidhya/aws-s3-multipart-upload-download-using-boto3-python-sdk-2dedb0945f11
# source: https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/s3/transfer.html
def _s3_upload_file(s3, file_path, bucket, key):
    suffix = Path(file_path).suffix
    if suffix:
        extension=suffix[1:]
    s3.Object(bucket, key).upload_file(file_path,
                            ExtraArgs={'ContentType': f'text/{extension}'},
                            Config=multipart_config,
                            Callback=ProgressPercentage(file_path)
                            )
    print('\n')

 # source: https://medium.com/analytics-vidhya/aws-s3-multipart-upload-download-using-boto3-python-sdk-2dedb0945f11
 # source: https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/s3/transfer.html
def _s3_download_file(s3, s3_path=None, bucket=None, key=None, path_to_download='.'):
    if s3_path:
        bucket, key =  s3_path.split('/',3)[2:]
        filename = key.split('/')[-1]
    else:
        filename = key.split('/')[-1]
    file_path = os.path.join(path_to_download,filename)
    s3.Object(bucket, key).download_file(file_path,
                            Config=multipart_config,
                            Callback=ProgressPercentage(file_path)
                            )
    print('\n')
    print("File downloaded to the path:", f"{file_path}")

def _s3_writer(s3, df, bucket, filename, extension, pandas_args = {}, polars_args = {}):
    buf = BytesIO()
    suffix = Path(filename).suffix
    if suffix:
        extension = suffix[1:]
    extension = extension.lower()
    if which_dataframe(df)=='pandas':
        if extension=='csv':
            df.to_csv(buf, **pandas_args)
        elif extension=='parquet':
            df.to_parquet(buf, **pandas_args)
        elif extension=='json':
            df.to_json(buf, **pandas_args)
        elif extension=='feather':
            df.to_feather(buf, **pandas_args)
        elif extension in ['xlsx','xls']:
            df.to_excel(buf, **pandas_args)
        else:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    elif which_dataframe(df)=='polars':
        if extension=='csv':
            df.write_csv(buf, **polars_args)
        elif extension=='parquet':
            df.write_parquet(buf, **polars_args)
        elif extension=='avro':
            df.write_avro(buf, **polars_args)
        elif extension=='json':
            df.write_json(buf, **polars_args)
        elif extension in ['feather','arrow']:
            df.write_ipc(buf, **polars_args)
        elif extension in ['xlsx','xls']:
            df.write_excel(buf, **polars_args)
        else:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    buf.seek(0)
    s3.Bucket(bucket).put_object(Key=filename, Body=buf.getvalue())
    
def _gcs_writer(gcs, df, bucket, filename, extension, pandas_args = {}, polars_args = {}):
    buf = BytesIO()
    suffix = Path(filename).suffix
    if suffix:
        extension = suffix[1:]
    bucket = gcs.get_bucket(bucket)
    extension = extension.lower()
    if which_dataframe(df)=='pandas':
        if extension=='csv':
            bucket.blob(filename).upload_from_string(df.to_csv(**pandas_args), 'text/csv')
        elif extension=='parquet':
            bucket.blob(filename).upload_from_string(df.to_parquet(**pandas_args), 'text/parquet')
        elif extension=='json':
            bucket.blob(filename).upload_from_string(df.to_json(**pandas_args), 'text/json')
        # elif extension=='feather':
        #     bucket.blob(filename).upload_from_string(df.to_feather(), 'text/feather')
        # elif extension in ['xlsx','xls']:
        #     bucket.blob(filename).upload_from_string(df.to_excel(), 'text/xlsx')
        else:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    elif which_dataframe(df)=='polars':
        if extension=='csv':
            bucket.blob(filename).upload_from_string(df.write_csv(**polars_args), 'text/csv')
        elif extension=='parquet':
            df.write_parquet(buf, **polars_args)
            buf.seek(0)
            bucket.blob(filename).upload_from_file(buf)
        elif extension=='json':
            bucket.blob(filename).upload_from_string(df.write_json(**polars_args), 'text/json')
        elif extension=='avro':
            df.write_avro(buf, **polars_args)
            buf.seek(0)
            bucket.blob(filename).upload_from_file(buf)
        # elif extension in ['feather','arrow']:
        #     df.write_ipc(buf, **polars_args)
        elif extension in ['xlsx','xls']:
            bucket.blob(filename).upload_from_string(df.write_excel(**polars_args), 'text/excel')
        else:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')

def _azure_blob_writer(abs, df, container_name,blob_name, extension, overwrite=True, pandas_args = {}, polars_args = {}):
    suffix = Path(blob_name).suffix
    if suffix:
        extension = suffix[1:]
    container_client = abs.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    buf = BytesIO()
    if which_dataframe(df) == 'pandas':
        if extension=='csv':
            df.to_csv(buf, **pandas_args)
        elif extension=='json':
            df.to_json(buf, **pandas_args)
        elif extension=='parquet':
            df.to_parquet(buf, **pandas_args)
        elif extension=='feather':
            df.to_feather(buf, **pandas_args)
        elif extension in ['xlsx','xls']:
            df.to_excel(buf, **pandas_args)
        else:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    elif which_dataframe(df)=='polars':
        if extension=='csv':
            df.write_csv(buf, **polars_args)
        elif extension=='parquet':
            df.write_parquet(buf, **polars_args)
        elif extension=='avro':
            df.write_avro(buf, **polars_args)
        elif extension=='json':
            df.write_json(buf, **polars_args)
        elif extension in ['feather','arrow']:
            df.write_ipc(buf, **polars_args)
        elif extension in ['xlsx','xls']:
            df.write_excel(buf, **polars_args)
        else:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=overwrite)

# source: https://medium.com/analytics-vidhya/aws-s3-multipart-upload-download-using-boto3-python-sdk-2dedb0945f11
# source: https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/s3/transfer.html
class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()
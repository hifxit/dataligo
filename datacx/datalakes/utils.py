from io import BytesIO
from pathlib import Path
import pandas as pd
from ..exceptions import ExtensionNotSupportException
from boto3.s3.transfer import TransferConfig

multipart_config = TransferConfig(multipart_threshold=1024 * 50, 
                        max_concurrency=8,
                        multipart_chunksize=1024 * 10,
                        use_threads=True)

def _bytes_to_df(body,extension,reader):
    if extension=='csv':
        df = reader(body, encoding='utf-8')
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

def _s3_upload_file(s3, file_path, bucket, key,):
    suffix = Path(file_path).suffix
    if suffix:
        extension=suffix[1:]
    s3.Object(bucket, key).upload_file(file_path,
                            ExtraArgs={'ContentType': f'text/{extension}'},
                            Config=multipart_config
                            )

def _s3_writer(s3, df, bucket, filename, extension, index=False, sep=','):
    buf = BytesIO()
    suffix = Path(filename).suffix
    if suffix:
        extension = suffix[1:]
    extension = extension.lower()
    if extension=='csv':
        df.to_csv(buf, index=index, sep=sep)
    elif extension=='parquet':
        df.to_parquet(buf, index=index)
    elif extension=='json':
        df.to_json(buf)
    elif extension=='feather':
        df.to_feather(buf)
    elif extension in ['xlsx','xls']:
        df.to_parquet(buf, index=index)
    else:
        raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    buf.seek(0)
    s3.Bucket(bucket).put_object(Key=filename, Body=buf.getvalue())
    
def _gcs_writer(gcs, df, bucket, filename, extension, index=False, sep=','):
    suffix = Path(filename).suffix
    if suffix:
        extension = suffix[1:]
    bucket = gcs.get_bucket(bucket)
    extension = extension.lower()
    if extension=='csv':
        bucket.blob(filename).upload_from_string(df.to_csv(index=index, sep=sep), 'text/csv')
    elif extension=='parquet':
        bucket.blob(filename).upload_from_string(df.to_parquet(index=index), 'text/parquet')
    elif extension=='json':
        bucket.blob(filename).upload_from_string(df.to_json(), 'text/json')
    # elif extension=='feather':
    #     bucket.blob(filename).upload_from_string(df.to_feather(), 'text/feather')
    # elif extension in ['xlsx','xls']:
    #     bucket.blob(filename).upload_from_string(df.to_excel(), 'text/xlsx')
    else:
        raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')

def _azure_blob_writer(abs, df, container_name,blob_name, extension, overwrite=True, index=False, sep=','):
    suffix = Path(blob_name).suffix
    if suffix:
        extension = suffix[1:]
    container_client = abs.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    buf = BytesIO()
    if extension=='csv':
        df.to_csv(buf,encoding='utf-8',index=index,sep=sep)
    elif extension=='json':
        df.to_json(buf)
    elif extension=='parquet':
        df.to_parquet(buf)
    elif extension=='feather':
        df.to_feather(buf)
    elif extension in ['xlsx','xls']:
        df.to_excel(buf)
    else:
        raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=overwrite)

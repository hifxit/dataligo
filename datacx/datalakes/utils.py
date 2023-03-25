from io import BytesIO
from pathlib import Path
from ..exceptions import ExtensionNotSupportException

def _bytes_to_df(body,extension,reader):
    if extension=='csv':
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

def _s3_writer(s3, df, bucket, filename, extension, index=False, sep=','):
    buf = BytesIO()
    suffix = Path(filename).suffix
    if suffix:
        extension = suffix[1:]
    extension = extension.lower()
    if extension=='csv':
        df.to_csv(buf, index=index, sep=sep)
        buf.seek(0)
        s3.Bucket(bucket).put_object(
            Key=filename, Body=buf.getvalue()
        )
    elif extension=='parquet':
        df.to_parquet(buf, index=index)
        buf.seek(0)
        s3.Bucket(bucket).put_object(
            Key=filename, Body=buf.getvalue()
        )
    elif extension=='json':
        df.to_json(buf)
        buf.seek(0)
        s3.Bucket(bucket).put_object(
            Key=filename, Body=buf.getvalue()
        )
    elif extension=='feather':
        df.to_feather(buf)
        buf.seek(0)
        s3.Bucket(bucket).put_object(
            Key=filename, Body=buf.getvalue()
        )
    elif extension in ['xlsx','xls']:
        df.to_parquet(buf, index=index)
        buf.seek(0)
        s3.Bucket(bucket).put_object(
            Key=filename, Body=buf.getvalue()
        )
    else:
        raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
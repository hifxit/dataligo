import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
from typing import Dict
import pandas as pd
from io import BytesIO
from pathlib import Path
from .utils import (_s3_writer, _multi_file_load, _gcs_writer,
                     _azure_blob_writer, _s3_upload_file, 
                    _s3_download_file, _s3_upload_folder, _s3_download_folder, readers, df_concat)
from ..exceptions import ExtensionNotSupportException
import os


class S3():
    def __init__(self,config):
        """
        S3 class create a ligo s3 object, through which you can able to read, write, upload, download data from AWS S3

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._s3 = boto3.resource(
            "s3",
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
        )

    def read_as_dataframe(self,s3_path: str = None, bucket: str = None, key: str = None, pandas_args: Dict = {}, 
                            polars_args: Dict = {}, extension='csv', return_type='pandas'):
        """
        Takes s3 path as arguments and return dataframe.

        Args:
            s3_path (str): s3 path of the file need to be loaded, for multiple file loading, use s3://bucket/path/filename*
                           to load all files from folder, use s3://bucket/folder/.
            bucket (str): S3 Bucket Name
            key (str): file name with extension
            pandas_args (dict): pandas arguments like encoding, etc
            extension (str, optional): extension of the files, It take automatically from the s3_path parameter. Defaults to 'csv'.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        if return_type=='polars':
            import polars as pl
            reader_args = polars_args
        elif return_type=='pandas':
            reader_args = pandas_args
        _readers = readers(return_type)
        if s3_path:
            suffix = Path(s3_path).suffix
        else:
            suffix = Path(key).suffix
        if suffix:
            extension = suffix[1:]
        if extension not in _readers:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
        reader = _readers[extension]
        if s3_path:
            bucket, key =  s3_path.split('/',3)[2:]
        if key.endswith('*') or key.endswith('/*') or key.endswith('/'):
            dfs = _multi_file_load(self._s3,bucket=bucket,key=key,reader=reader,extension=extension,reader_args=reader_args)
            return df_concat(dfs,return_type)
        else:
            obj = self._s3.Object(bucket_name=bucket, key=key)
            stream = BytesIO(obj.get()['Body'].read())
            df = reader(stream, **reader_args)
            return df
        
    def write_dataframe(self, df, bucket: str, key: str, extension='csv', pandas_args = {}, polars_args = {}) -> None:
        """
        Takes DataFrame, bucket name, filename as arguments and write the dataframe to S3.

        Args:
            df (DataFrame): Dataframe which need to be uploaded
            bucket (str): S3 Bucket Name
            key (str): file name with extension
            extension (str, optional): extension of the files, It take automatically from the filename parameter. Defaults to 'csv'
            index (bool, optional): pandas index parameter. Defaults to False.
            sep (str, optional): pandas sep parameter. Defaults to ','.
        """
        _s3_writer(self._s3, df, bucket, key, extension, pandas_args = pandas_args, polars_args = polars_args)
        print("Dataframe saved to the s3 path:", f"s3://{bucket}/{key}")

    def upload_file(self, source_file_path: str, bucket: str, key: str):
        """
        Takes source file path, bucket and key as arguments and upload the file to S3

        Args:
            source_file_path (str): source file path
            bucket (str): destination bucket
            key (str): destination file path
        """
        _s3_upload_file(self._s3, file_path=source_file_path, bucket=bucket, key=key)
        print("File uploaded to the s3 path:", f"s3://{bucket}/{key}")

    def download_file(self, s3_path: str = None, bucket: str = None, key: str = None, local_path_to_download: str = '.'):
        """
        Takes s3 path or (bucket and key name) as arguments and download the file

        Args:
            s3_path (str, optional): S3 path from where it needs to download the file. Defaults to None.
            bucket (str, optional): S3 bucket name, if S3 path is not provided . Defaults to None.
            key (str, optional): S3 Key name, if S3 path is not provied. Defaults to None.
            path_to_download (str, optional): save location. Defaults to '.' (current directory).
        """
        _s3_download_file(self._s3, s3_path=s3_path,bucket=bucket, key=key,path_to_download=local_path_to_download)

    def upload_folder(self, local_folder_path: str, bucket: str, key: str) -> None:
        """
        Takes local path, bucket and key as arguments and upload the folder to s3

        Args:
            local_folder_path (str): local path of the folder want to be uploaded
            bucket (str): s3 bucket name
            key (str): s3 key name
        """
        _s3_upload_folder(self._s3,local_folder_path, bucket, key)
        print("Folder uploaded to the s3 path:", f"s3://{bucket}/{key}")

    def download_folder(self, s3_path: str = None, bucket: str = None, key: str = None, local_path_to_download: str = '.'):
        """
        Takes s3 path or (bucket and key name) as arguments and download the folder

        Args:
            s3_path (str, optional): S3 path from where it needs to download the folder. Defaults to None.
            bucket (str, optional): S3 bucket name, if S3 path is not provided . Defaults to None.
            key (str, optional): S3 Key name, if S3 path is not provied. Defaults to None.
            local_path_to_download (str, optional): save location. Defaults to '.' (current directory).
        """
        _s3_download_folder(self._s3, s3_path=s3_path, bucket=bucket,key=key,local_path_to_download=local_path_to_download)

class GCS():
    def __init__(self,config):
        """
        GCS class create a ligo gcs object, through which you can able to read, write, upload, download data from Google Cloud Storage.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._gcs = storage.Client.from_service_account_json(json_credentials_path=config['GOOGLE_APPLICATION_CREDENTIALS_PATH'])

    def read_as_dataframe(self, gcs_path: str = None, bucket: str = None, blob_name: str = None, pandas_args: Dict = {}, 
                            polars_args: Dict = {}, extension='csv', return_type='pandas'):
        """Takes gcs path as argument and return dataframe.

        Args:
            gcs_path (str): gcs path of the file need to be loaded, for multiple file loading, use gs://bucket/path/filename*
                           to load all files from folder, use gs://bucket/folder/.
            bucket (str): GCS Bucket Name
            blob_name (str): file name with extension
            pandas_args (dict): pandas arguments like encoding, etc
            extension (str, optional): extension of the files, It take automatically from the gcs path parameter. Defaults to 'csv'.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        if return_type=='polars':
            import polars as pl
            reader_args = polars_args
        elif return_type=='pandas':
            reader_args = pandas_args
        _readers = readers(return_type)
        if gcs_path:
            suffix = Path(gcs_path).suffix
        else:
            suffix = Path(blob_name).suffix
        if suffix:
            extension = suffix[1:]
        if extension not in _readers:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
        reader = _readers[extension]
        if gcs_path:
            bucket, blob_name = gcs_path.split('/',3)[2:]
        bucket = self._gcs.get_bucket(bucket)
        if blob_name.endswith('/') or blob_name.endswith('/*') or blob_name.endswith('*'):
            blob_name = blob_name.strip('*')
            blob_names = [blob.name for blob in bucket.list_blobs()]
            dfs = []
            for blob in blob_names:
                if blob.startswith(blob_name):
                    extension = Path(blob).suffix[1:]
                    reader = _readers[extension]
                    blob = bucket.blob(blob)
                    data = blob.download_as_string()
                    stream = BytesIO(data)
                    df = reader(stream, **reader_args)
                    dfs.append(df)
            return df_concat(dfs,return_type)
        else:
            blob = bucket.blob(blob_name)
            data = blob.download_as_string()
            stream = BytesIO(data)
            df = reader(stream, **reader_args)
            return df

    def write_dataframe(self, df, bucket, blob_name, extension='csv', pandas_args = {}, polars_args = {}):
        """
        Takes DataFrame, bucket name, blob name as arguments and write the dataframe to GCS.

        Args:
            df (DataFrame): Dataframe which need to be uploaded
            bucket (str): GCS Bucket Name
            blob_name (str): file name with extension
            extension (str, optional): extension of the files, It take automatically from the filename parameter. Defaults to 'csv'
            index (bool, optional): pandas index parameter. Defaults to False.
            sep (str, optional): pandas sep parameter. Defaults to ','.
        """
        _gcs_writer(self._gcs,df,bucket=bucket,filename=blob_name,extension=extension, pandas_args = pandas_args, polars_args = polars_args)
        print("Dataframe saved to the gcs path:", f"gs://{bucket}/{blob_name}")
    
    def upload_file(self, source_file_path: str, bucket: str, blob_name: str):
        """
        Takes source file path, bucket and blob name as arguments and upload the file to GCS

        Args:
            source_file_path (str): Source file path
            bucket (str): GCS Bucket Name
            blob_name (str): Blob name (destination file path)
        """
        Bucket = storage.Bucket(self._gcs, bucket)
        blob = Bucket.blob(blob_name)
        blob.upload_from_filename(source_file_path)
        print("File uploaded to the gcs path:", f"gs://{bucket}/{blob_name}")

    def download_file(self, gcs_path: str = None, bucket: str = None, blob_name: str = None, path_to_download: str = '.'):
        """
        Takes gcs path or (bucket and blob name) as arguments and download the file

        Args:
            gcs_path (str, optional): GCS file path. Defaults to None.
            bucket (str, optional): GCS bucket name, if gcs path is not provided. Defaults to None.
            blob_name (str, optional): GCS blob name, if gcs path is not provied. Defaults to None.
            path_to_download (str, optional): save location. Defaults to '.'.
        """
        if gcs_path:
            bucket, blob_name = gcs_path.split('/',3)[2:]
        Bucket = self._gcs.get_bucket(bucket)
        blob = Bucket.blob(blob_name)
        filename = Path(blob_name).name
        file_path = os.path.join(path_to_download,filename)
        blob.download_to_filename(file_path)
        print("File downloaded to the path:", f"{file_path}")
        
    def upload_folder(self,local_folder_path: str, bucket: str, blob_path: str=''):
        bucket = self._gcs.get_bucket(bucket)
        for path, _, files in os.walk(local_folder_path):
            for name in files:
                local_path = os.path.join(path, name)
                relative_path = os.path.relpath(local_path, local_folder_path).replace('\\', '/')
                dest_blob_path = local_path.replace(local_folder_path, "").replace('\\', '/')
                if blob_path:
                    dest_blob_path = os.path.join(blob_path, relative_path)
                blob = bucket.blob(dest_blob_path)
                blob.upload_from_filename(local_path)
        print('Folder uploaded to the bucket: ', f"{bucket}/{blob_path}")

    def download_folder(self, gcs_path: str = None, bucket: str = None, blob_path: str = None, local_path_to_download: str = '.'):
        if gcs_path:
            bucket, blob_path = gcs_path.split('/',3)[2:]
        bucket = self._gcs.get_bucket(bucket)
        os.makedirs(os.path.join(local_path_to_download,Path(blob_path).stem))
        for blob in bucket.list_blobs(prefix=blob_path):
            if '.' in blob.name:
                filepath = os.path.join(local_path_to_download, blob.name)
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(filepath)
        print('Folder downloaded to the path:',f"{local_path_to_download}/{Path(blob_path).stem}")

class AzureBlob():
    def __init__(self,config):
        """
        AzureBlob class create a ligo azureblob object, through which you can able to read, write, upload, download data from Azure Blob Storage.

        Args:
            config (dict): Automatically loaded from the config file (yaml)
        """
        self._abs = BlobServiceClient(account_url=f"https://{config['ACCOUNT_NAME']}.blob.core.windows.net",
                                        credential=config['ACCOUNT_KEY'])
        
    def read_as_dataframe(self, container_name: str,blob_name: str, pandas_args: Dict = {}, 
                            polars_args: Dict = {}, extension='csv', return_type='pandas'):
        """Takes Azure Storage account container name and blob name and return datafarme.

        Args:
            container_name (str): Container Name of the azure storage account 
            blob_name (str): Blob Name which wants to read
            pandas_args (dict): pandas arguments like encoding, etc
            extension (str, optional): extension of the files, It take automatically from the blob_name parameter. Defaults to 'csv'.
            return_type (str, optional): which dataframe you want to return (pandas, polars, dask etc). Defaults to 'pandas'.

        Returns:
            DataFrame: Depends on the return_type parameter.
        """
        if return_type=='polars':
            import polars as pl
            reader_args = polars_args
        elif return_type=='pandas':
            reader_args = pandas_args
        _readers = readers(return_type)
        suffix = Path(blob_name).suffix
        if suffix:
            extension = suffix[1:]
        if extension not in _readers:
            raise ExtensionNotSupportException(f'Unsupported Extension: {extension}')
        reader = _readers[extension]
        container_client = self._abs.get_container_client(container_name)
        if blob_name.endswith('/') or blob_name.endswith('/*') or blob_name.endswith('*'):
            blob_name = blob_name.strip('*')
            blob_names = [name for name in container_client.list_blob_names()]
            dfs = []
            for blob in blob_names:
                if blob.startswith(blob_name):
                    blob_client = container_client.get_blob_client(blob)
                    stream = BytesIO(blob_client.download_blob().readall())
                    extension = Path(blob).suffix[1:]
                    reader = _readers[extension]
                    df = reader(stream, **reader_args)
                    dfs.append(df)
            return df_concat(dfs,return_type)
        else:
            blob_client = container_client.get_blob_client(blob_name)
            stream = BytesIO(blob_client.download_blob().readall())
            df = reader(stream, **reader_args)
            return df
        
    def write_dataframe(self, df, container_name: str, blob_name: str, overwrite=True, extension='csv', pandas_args = {}, polars_args = {}):
        """Takes DataFrame, container name, filename as arguments and write the dataframe to Azure Blob Storage.

        Args:
            df (DataFrame): Dataframe which need to be uploaded
            container_name (str): Container Name of the azure storage account
            filename (str): file name with extension
            overwrite (bool, optional): Overwrite the existing data. Defaults to True.
            extension (str, optional): extension of the files, It take automatically from the filename parameter. Defaults to 'csv'
            index (bool, optional): pandas index parameter. Defaults to False.
            sep (str, optional): pandas sep parameter. Defaults to ','.
        """
        _azure_blob_writer(self._abs, df, container_name,blob_name,overwrite=overwrite,extension=extension, pandas_args = {}, polars_args = {})
        print("Dataframe saved to the container", container_name, "with the blob name of", blob_name)

    # source: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    def upload_file(self,source_file_path: str, container_name: str, blob_name: str = None):
        """
        Takes source file path, container name and blob name as arguments and upload the file to Azure Blob Storage

        Args:
            source_file_path (str): source file path
            container_name (str): container name
            blob_name (str, optional): blob name, if not mentioned, it automatically takes source filename as blob name. Defaults to None.
        """
        if blob_name:
            blob_client = self._abs.get_blob_client(container=container_name, blob=blob_name)
        else:
            filename = Path(source_file_path).name
            blob_client = self._abs.get_blob_client(container=container_name, blob=filename)
        with open(source_file_path,'rb') as data:
            blob_client.upload_blob(data)
        print("File uploaded to the container", container_name, "with the blob name of", blob_name)

    # source: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    def download_file(self,container_name: str, blob_name: str, path_to_download='.'):
        """
        Takes container name and blob name as arguments and download the file

        Args:
            container_name (str): container name
            blob_name (str): blob name
            path_to_download (str, optional): save location. Defaults to '.'.
        """
        filename = Path(blob_name).name
        download_file_path = os.path.join(path_to_download, filename)
        container_client = self._abs.get_container_client(container= container_name)
        with open(file=download_file_path, mode="wb") as download_file:
            download_file.write(container_client.download_blob(blob_name).readall())
        print('File downloaded to the path:', download_file_path)

    def upload_folder(self, local_folder_path: str, container_name: str, blob_name: str) -> None:
        container_client = self._abs.get_container_client(container_name)
        for root, _, files in os.walk(local_folder_path):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_folder_path)
                blob_name = relative_path.replace("\\", "/")
                blob_client = container_client.get_blob_client(blob_name)
                with open(local_path, "rb") as data:
                    blob_client.upload_blob(data)
        print("Folder uploaded to the container", container_name, "with the blob name of", Path(local_folder_path).stem)

    def download_folder(self,container_name: str, blob_path: str, local_path_to_download='.'):
        container_client = self._abs.get_container_client(container_name)
        for blob in container_client.list_blob_names():
            if blob.startswith(blob_path):
                blob_client = container_client.get_blob_client(blob)
                file_path = os.path.join(local_path_to_download, blob)
                print(file_path)
                os.makedirs(os.path.dirname(file_path),exist_ok=True)
                with open(file_path, "wb") as download_file:
                    download_stream = blob_client.download_blob()
                    download_file.write(download_stream.readall())
        print('Folder downloaded to the path:', f"{local_path_to_download}/{Path(blob_path).stem}")
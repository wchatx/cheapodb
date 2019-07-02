import os
import re
import logging
from urllib.parse import urlencode
from typing import List

from dask.dataframe import DataFrame

from cheapodb.database import Database
from cheapodb.utils import normalize_table_name


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S'
)

log = logging.getLogger(__name__)


class Table(object):
    """
    A Table object represents the components that make up a table in AWS Glue.

    Provides methods for Glue, Athena and S3
    """
    def __init__(self, db: Database, name: str, prefix: str):
        """
        Create a Table instance

        :param db: the Database that will contain the Table
        :param name: the name of the Table
        :param prefix: the prefix in the Database where the Table data will reside
        """
        self.db = db
        self.name = normalize_table_name(name)
        self.prefix = prefix

    @property
    def columns(self) -> List[dict]:
        """
        Get a list of table columns

        :return: list of dicts describing the table columns
        """
        return self.describe()['Table']['StorageDescriptor']['Columns']

    def get_versions(self) -> List[dict]:
        """
        Get a list of table versions

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table_versions

        :return:
        """
        versions = list()
        while True:
            payload = dict(
                DatabaseName=self.db.name,
                TableName=self.name,
                MaxResults=100
            )
            response = self.db.glue.get_table_versions(**payload)
            if not response['TableVersions']:
                break
            if response['NextToken']:
                payload['NextToken'] = response['NextToken']
            versions += response['TableVersions']

        return versions

    def describe(self) -> dict:
        """
        Get a reference to the table with metadata.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table

        :return: the Glue get_table response
        """
        return self.db.glue.get_table(
            DatabaseName=self.db.name,
            Name=self.name
        )

    def upload(self, f, tags: dict = None) -> None:
        """
        Upload the table data file(s) to the database bucket and prefix

        :param f: path to the file to upload
        :param tags: an optional dict of key:value tags to apply to the uploaded object
        :return:
        """
        target = os.path.join(self.prefix, self.name, self.name)
        log.info(f'Uploading file {f} to {target}')

        extra_args = None
        if tags:
            extra_args = dict(
                Tagging=urlencode(tags)
            )

        self.db.bucket.upload_file(f, target, ExtraArgs=extra_args)
        return

    def download(self, f) -> None:
        """
        Download the originally uploaded table data file to the specified path

        :param f: path where downloaded file will be stored
        :return:
        """
        target = os.path.join(self.prefix, self.name, self.name)
        log.info(f'Downloading file {target} to {f}')
        self.db.bucket.download_file(target, f)
        return

    def as_parquet(self, df: DataFrame, **kwargs) -> None:
        """
        Load a Dask DataFrame as parquet to the database bucket and prefix

        :param df: the Dask DataFrame object to load as parquet
        :param kwargs: additional keyword arguments provided to Dask DataFrame.to_parquet
        :return:
        """
        target = f's3://{os.path.join(self.db.name, self.prefix, self.name)}'
        df.to_parquet(path=target, **kwargs)
        return

    def as_json(self, df: DataFrame, compression=None, **kwargs) -> None:
        """

        :param df:
        :param compression:
        :param kwargs:
        :return:
        """
        target = f's3://{os.path.join(self.db.name, self.prefix, self.name)}'
        df.to_json(target, compression=compression, **kwargs)
        return

    def delete_table(self, include_data: bool = True) -> None:
        """
        Delete a table in a Glue database

        :param include_data: True to include the underlying data in S3
        :return:
        """
        if include_data:
            d = f'{self.prefix}/{self.name}'
            log.info(f'Deleting data at {d}')
            try:
                versions = self.db.s3.list_object_versions(
                    Bucket=self.db.name,
                    Prefix=d
                )['Versions']
                actions = [self.db.s3.delete_object(
                    Bucket=self.db.name,
                    Key=x['Key'],
                    VersionId=x['VersionId']
                ) for x in versions]
                log.debug(actions)
                log.info(f'Deleted data at {d}')
            except KeyError:
                log.warning(f'Data does not exist at {self.db.name}/{d}')
        try:
            log.info(f'Deleting table {self.prefix}_{self.name}')
            response = self.db.glue.delete_table(
                DatabaseName=self.db.name,
                Name=f'{self.prefix}_{self.name}'
            )
            log.debug(response)
            log.info(f'Deleted table {self.prefix}_{self.name}')
        except self.db.glue.exceptions.EntityNotFoundException:
            log.warning(f'Table does not exist in {self.db.name}')
        return

    def from_dataframe(self, df: DataFrame, **kwargs) -> None:
        """
        Load a Dask DataFrame as parquet to the database bucket and prefix

        :param df: the Dask DataFrame object to load as parquet
        :param kwargs: additional keyword arguments provided to Dask DataFrame.to_parquet
        :return:
        """
        target = f's3://{os.path.join(self.db.name, self.prefix, self.name)}'
        while True:
            try:
                df.to_parquet(path=target, engine='fastparquet', dtype=lambda x: dtype if dtype else None, **kwargs)
                break
            except ValueError as e:
                dtype = dict()
                type_errors = re.findall(
                    pattern=r'\| (.*)( )*\| ([a-z]*) \|',
                    string=str(e)
                )
                dtype.update({x[0].strip(): x[2] for x in type_errors})

        return

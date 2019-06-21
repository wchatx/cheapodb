import json
import logging
import time
from itertools import islice, chain
from typing import Union, Generator, List

from joblib import Parallel, delayed
import backoff

from cheapodb import Database


log = logging.getLogger(__name__)


class Stream(object):
    def __init__(self, name: str, db: Database):
        self.name = name
        self.db = db

    @backoff.on_exception(backoff.expo,
                          Exception,
                          max_tries=8)
    def initialize(self, buffering: dict = None, compression: str = 'UNCOMPRESSED', prefix: str = None,
                   error_prefix: str = None):
        if self.exists:
            return

        if not buffering:
            buffering = dict(
                SizeInMBs=5,
                IntervalInSeconds=300
            )
        s3config = dict(
            RoleARN=self.db.iam_role_arn,
            BucketARN=f'arn:aws:s3:::{self.db.name}',
            BufferingHints=buffering,
            CompressionFormat=compression,

        )
        if prefix:
            s3config['Prefix'] = prefix
        if error_prefix:
            s3config['ErrorOutputPrefix'] = error_prefix
        payload = dict(
            DeliveryStreamName=self.name,
            DeliveryStreamType='DirectPut',
            ExtendedS3DestinationConfiguration=s3config
        )
        response = self.db.firehose.create_delivery_stream(**payload)
        while True:
            status = self.exists
            if status and status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
                break

            elif status and status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'CREATING':
                time.sleep(10)
                continue

            else:
                raise Exception(f'Delivery stream status was not one of ACTIVE or CREATING')

        return response

    @property
    def describe(self) -> dict:
        response = self.db.firehose.describe_delivery_stream(
            DeliveryStreamName=self.name
        )
        return response

    @property
    def exists(self) -> Union[dict, None]:
        try:
            response = self.describe
            log.debug(response)
            return response
        except self.db.firehose.exceptions.ResourceNotFoundException:
            return None

    @staticmethod
    def _chunks(iterable: Union[Generator, list], size: int):
        iterator = iter(iterable)
        for first in iterator:
            yield chain([first], islice(iterator, size - 1))

    def from_records(self, records: Union[Generator, List[dict]], threads: int = 4) -> None:
        """
        Ingest from a generator or list of dicts

        :param records: a generator or list of dicts
        :param threads: number of threads for batch putting
        :return:
        """
        Parallel(n_jobs=threads, prefer='threads')(delayed(self.db.firehose.put_record_batch)(
            DeliveryStreamName=self.name,
            Records=[{'Data': json.dumps(x).encode()} for x in chunk]
        ) for chunk in self._chunks(records, size=500))

        return

import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)


class CheapoDBException(Exception):
    pass


def normalize_table_name(name):
    """Check if the table name is obviously invalid."""
    if not isinstance(name, str):
        raise ValueError()
    name = name.strip()
    if not len(name):
        raise ValueError(f'Invalid table name: {name}')
    return name


def create_cheapodb_role(name, client, bucket: str, account: str) -> str:
    """
    Create an AWS IAM service role with the appropriate permissions for Glue and the database's S3 bucket.

    :param name:
    :param client:
    :param bucket:
    :param account:
    :return:
    """
    log.info(f'Creating role. bucket: {bucket}, account: {account}')
    try:
        response = client.create_role(
            RoleName=name,
            Path='/service-role/',
            Description=f'IAM role created by CheapoDB on {datetime.now():%Y-%m-%d %H:%M:%S}',
            AssumeRolePolicyDocument=json.dumps(dict(
                Version='2012-10-17',
                Statement=[
                    {
                        'Sid': '',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'glue.amazonaws.com'
                        },
                        'Action': 'sts:AssumeRole'
                    },
                    {
                        'Sid': '',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'firehose.amazonaws.com'
                        },
                        'Action': 'sts:AssumeRole',
                        'Condition': {
                            'StringEquals': {
                                'sts:ExternalId': account
                            }
                        }
                    }
                ]
            ))
        )
        iam_role_arn = response['Role']['Arn']

        response = client.attach_role_policy(
            RoleName=name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )
        log.debug(response)

        response = client.put_role_policy(
            RoleName=name,
            PolicyName='CheapoDBRolePolicy',
            PolicyDocument=json.dumps(dict(
                Version='2012-10-17',
                Statement=[
                    {
                        'Effect': 'Allow',
                        'Action': [
                            's3:GetObject',
                            's3:PutObject',
                            's3:AbortMultipartUpload',
                            's3:GetBucketLocation',
                            's3:ListBucket',
                            's3:ListBucketMultipartUploads',
                        ],
                        'Resource': [
                            f'arn:aws:s3:::{bucket}',
                            f'arn:aws:s3:::{bucket}/'
                        ]
                    },
                    {
                        'Effect': 'Allow',
                        'Action': [
                            'firehose:CreateDeliveryStream',
                            'firehose:DeleteDeliveryStream',
                            'firehose:PutRecord',
                            'firehose:PutRecordBatch',
                            'firehose:UpdateDestination'
                        ],
                        'Resource': [
                            f'arn:aws:firehose:us-east-1:{account}:deliverystream/*'
                        ]
                    }
                ]
            ))
        )
        log.debug(response)
    except client.exceptions.EntityAlreadyExistsException:
        msg = f'Role already exists for database: CheapoDBRole-{bucket}. ' \
            f'Provide the role ARN as iam_role_arn.'
        raise CheapoDBException(msg)

    return iam_role_arn

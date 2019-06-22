from dataclasses import dataclass


@dataclass
class SecurityConfiguration(object):
    name: str
    s3_encryption: str = 'DISABLED'
    s3_encryption_key_arn: str = None
    cloudwatch_encryption: str = 'DISABLED'
    cloudwatch_encryption_key_arn: str = None
    job_bookmarks_encryption: str = 'DISABLED'
    job_bookmarks_encryption_key_arn: str = None

    def create(self):
        pass
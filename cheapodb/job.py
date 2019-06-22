from cheapodb.utils import create_session


session = create_session()


class Command(object):
    location: str
    type: str = 'pythonshell'
    version: int = 3

    @staticmethod
    def upload(f: str, bucket: str, key: str) -> None:
        s3 = session.resource('s3')
        s3.meta.client.upload_file(f, bucket, key)
        return

    @property
    def payload(self) -> dict:
        return dict(
            Name=self.type,
            ScriptLocation=self.location,
            PythonVersion=self.version
        )


class Job(object):
    def __init__(self, name: str, iam_role_arn: str, command: Command):
        self.name = name
        self.iam_role_arn = iam_role_arn
        self.command = command
        self.glue = session.client('glue')

    def create(self):
        client = session.client('glue')
        payload = dict(
            Name=self.name,
            Role=self.iam_role_arn,
            Command=self.command.payload
        )
        response = client.create_job(**payload)


if __name__ == '__main__':
    cmd = Command(

    )



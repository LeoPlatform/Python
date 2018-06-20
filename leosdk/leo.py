import boto3

from leosdk.aws.cfg import Cfg
from leosdk.aws.leo_stream import LeoStream


class Leo:
    def __init__(self, leo_config, bot_id: str, queue_name: str):
        self.config = Cfg(leo_config)
        self.bot_id = bot_id
        self.queue_name = queue_name

    def load(self) -> LeoStream:
        return self.__loader()

    def __loader(self):
        writer_style = Leo.__writer_style(self.config)
        if writer_style == 'BATCH':
            from leosdk.aws.firehose import Firehose
            return Firehose(self.config)
        elif writer_style == 'STORAGE':
            from leosdk.aws.s3 import S3
            return S3(self.config)
        else:
            from leosdk.aws.kinesis import Kinesis
            return Kinesis(self.config)

    @staticmethod
    def __aws_session(config):
        profile = config.value('AWS_PROFILE')
        region = config.value('REGION')
        return boto3.Session(profile_name=profile, region_name=region)

    @staticmethod
    def __writer_style(config):
        uploader = config.value('WRITER')
        if uploader == 'BATCH' or uploader == 'STORAGE':
            return uploader
        else:
            return 'STREAM'

    @staticmethod
    def __writer_resource(config, writer_style):
        writer_resource = config.value(writer_style)
        if writer_resource is not None:
            return writer_resource
        else:
            raise AssertionError("Missing corresponding '%s' for current environment in leo_config.py")

    def create_offloader(self):
        raise NotImplementedError('Offloader not yet available')

    def create_enrichment(self):
        raise NotImplementedError('Enrichment not yet available')

from leosdk.aws.cfg import Cfg
from leosdk.aws.leo_stream import LeoStream
from leosdk.aws.payload import Payload


class S3(LeoStream):

    def __init__(self, config: Cfg):
        self.config = config
        # self.session = session
        # self.resource = resource

    def write(self, payload: Payload):
        pass

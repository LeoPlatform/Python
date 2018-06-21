from leosdk.aws.cfg import Cfg
from leosdk.aws.leo_stream import LeoStream
from leosdk.aws.payload import Payload


class S3(LeoStream):

    def __init__(self, config: Cfg, bot_id: str, queue_name: str):
        self.config = config
        self.bot_id = bot_id
        self.queue_name = queue_name
        # self.session = session
        # self.resource = resource

    def write(self, payload: Payload):
        pass

    def end(self):
        # if len(self.records) > 0:
        #     self.send_records()
        print('end')

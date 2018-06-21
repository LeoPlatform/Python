import time

from leosdk.aws.cfg import Cfg
from leosdk.aws.leo_stream import LeoStream
from leosdk.aws.payload import Payload


class Firehose(LeoStream):

    def __init__(self, config: Cfg, bot_id: str, queue_name: str):
        self.bot_id = bot_id
        self.queue_name = queue_name
        # array = Array()
        # self.id = nullid
        # self.region = region
        # self.client = session.client('firehose')
        # self.stream = stream
        self.combine = True
        self.batch_size = 3145728
        self.record_size = 734003
        self.max_records = 500
        self.duration = 100
        self.bytesPerSecond = 2097152
        # self.opts = array.defaults({
        #     'maxRetries': 4
        # }, opts)

    def write(self, payload: Payload):
        pass

    def send_records(self, batch):
        retries = 0
        correlation = None
        count = 0

        while len(batch['records']) > 0 and retries <= self.opts['maxRetries']:
            time_start = time.time()
            count = 0
            length = 0
            records = []

            for key in batch['records']:
                record = batch['records'][key]
                count += record['cnt']
                length += record['length']
                records.append({'Data': record['data']})

            result = self.client.put_record_batch(
                self.stream,
                records
            )

            if retries > 0:
                print('Retrying(#' + retries + ') ' + count + ' records of size (' + length + ') in ' + (
                        time.time() - time_start))
            else:
                print('Sent ' + count + ' records of size (' + length + ') in ' + (time.time() - time_start))

            has_errors = result.FailedPutCount

            if not has_errors:
                batch['records'] = []
            else:
                responses = result.RequestResponses
                print(responses)
                max_completed = -1

                for i in responses:
                    if responses[i]['RecordId']:
                        if max_completed == i - 1:
                            max_completed = i
                            correlation = batch['records'][max_completed]['correlation']

                        del batch['records'][i]

            retries += 1

        if len(batch['records']) > 0:
            return {
                'success': False,
                'errorMessage': 'Failed to write ' + str(len(batch['records'])) + ' events to the stream'
            }
        else:
            if correlation['end']:
                checkpoint = correlation['end']
            else:
                checkpoint = correlation['start']

            return {
                'success': True,
                'eid': checkpoint,
                'records': count
            }

    def end(self):
        # if len(self.records) > 0:
        #     self.send_records()
        print('end')

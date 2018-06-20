import gzip
import time
from typing import List

import boto3

from leosdk.aws.cfg import Cfg
from leosdk.aws.leo_stream import LeoStream
from leosdk.aws.payload import Payload


class Kinesis(LeoStream):
    size: int
    max_records: int
    max_retries: int
    records: List[Payload]

    def __init__(self, config: Cfg):
        self.stream_name = config.value('STREAM')
        if self.stream_name is None:
            raise AssertionError("Missing 'STREAM' field for the current environment in leo_config.py")

        self.client = Kinesis.__aws_session(config).client('kinesis')
        self.size = config.value_or_else('STREAM_MAX_BATCH_SIZE', 1024 * 200)
        self.max_records = config.value_or_else('STREAM_MAX_BATCH_RECORDS', 100)
        self.max_retries = config.value_or_else('STREAM_MAX_RETRIES', 10)
        self.records = []

    def write(self, payload: Payload):
        self.records.append(payload)
        if len(self.records) >= self.max_records:
            self.send_records()

    @staticmethod
    def __aws_session(config: Cfg):
        profile = config.value('AWS_PROFILE')
        region = config.value('REGION')
        return boto3.Session(profile_name=profile, region_name=region)

    def send_records(self):
        retries = 0

        while len(self.records) > 0 and retries <= self.max_retries:
            time_start = time.time()
            recs = self.compress_recs()

            result = self.client.put_records(
                Records=recs,
                StreamName=self.stream_name,
            )
            self.log_result(recs, retries, time_start)

            if not result.FailedRecordCount:
                self.records.clear()
            else:
                failed_indexes = self.failed_rec_indexes(result.Records)
                for i in failed_indexes:
                    del self.records[i]
                retries += 1

    @staticmethod
    def failed_rec_indexes(recs):
        failed_indexes = []
        for i in recs:
            if recs[i]['ErrorCode'] and recs[i]['ErrorMessage']:
                failed_indexes.append(int(recs[i]['SequenceNumber']))

        return failed_indexes

    @staticmethod
    def log_result(recs, retries, time_start):
        dur = time.time() - time_start
        if retries > 0:
            size = Kinesis.__calc_size(recs)
            print("Retrying (attempt %d) records of size (%d) in %d seconds" % (retries, size, dur))
        else:
            print("Sent %d records in %d seconds" % (len(recs), dur))

    def compress_recs(self):
        recs = []
        for r in self.records:
            recs.append({
                'Data': gzip.compress(r.get_payload_data()),
                'PartitionKey': r.get_id()
            })
        return recs

    @staticmethod
    def __calc_size(recs):
        size = 0
        for i in recs:
            size += len(recs[i]['Data'])
        return size

    def end(self):
        if len(self.records) > 0:
            self.send_records()
        print('end')

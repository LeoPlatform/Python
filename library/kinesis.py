from library.array import Array
import boto3
import time


class Kinesis:

	def __init__(self, partition_key, stream, region, opts={}):
		array = Array()
		self.client = boto3.client('kinesis')
		self.stream = stream
		self.id = partition_key
		self.region = region
		self.batch_size = 1048576
		self.record_size = 1048576
		self.max_records = 500
		self.duration = 10
		self.bytesPerSecond = 1572865
		self.combine = True
		self.opts = array.defaults({
			'maxRetries': 4
		}, opts)

	def send_records(self, batch):
		retries = 0
		count = 0
		correlation = None

		while len(batch['records']) > 0 and retries <= self.opts['maxRetries']:
			time_start = time.time()
			count = 0
			length = 0
			records = []

			for key in batch['records']:
				record = batch['records'][key]
				count += record['cnt']
				length += record['length']
				records.append({
					'Data': record['data'],
					'PartitionKey': self.id
				})

			print(self.id)

			result = self.client.put_records(
				self.stream,
				records
			)

			if retries > 0:
				print('Retrying(#' + retries + ') ' + count + ' records of size (' + length + ') in ' + (time.time() - time_start))
			else:
				print('Sent ' + count + ' records of size (' + length + ') in ' + (time.time() - time_start))

			has_errors = result.FailedRecordCount

			if not has_errors:
				batch['records'] = []
			else:
				responses = result.Records
				max_completed = -1

				for i in responses:
					if responses[i]['SequenceNumber']:
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

	@staticmethod
	def end():
		print('end')

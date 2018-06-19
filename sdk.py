#!/usr/local/bin/python3.6

import boto3
import json
from library.payload import Payload
from library.array import Array


class Sdk:
	def __init__(self, id, opts):
		array = Array()
		self.config = array.defaults({
			'region': 'us-west-2',
			'firehose': None,
			'kinesis': None,
			's3': None,
			'enableLogging': False,
			'uploader': 'kinesis',
			'debug': True
		}, opts)

		self.id = id
		self.session = None

		self.configuration()

	def configuration(self):
		# load the config file
		# self.config = json.load(open('config.json'))

		# set the AWS profile (should be set in the config file)
		self.session = boto3.Session(profile_name=self.config['aws_profile'])

		self.create_loader(self.config)
		print(self.session)

	def create_loader(self, checkpointer, opts = {}):
		if 'config' in opts:
			array = Array()
			opts['config'] = array.defaults(self.config, opts['config'])

		if opts['config']['uploader'] == 'firehose':
			from library.firehose import Firehose
			uploader = Firehose(self.id, opts)
		elif opts['config']['uploader'] == 'kinesis':
			from library.kinesis import Kinesis
			uploader = Kinesis(self.id, opts)

		from library.massuploader import Massuploader
		massuploader = Massuploader(self.id, opts, uploader)

		from library.combiner import Combiner
		return Combiner(self.id, opts, uploader, massuploader, checkpointer)

	def create_offloader(self, opts):
		print('Not implemented yet')

	def create_enrichment(self, opts):
		print('Not implemented yet')

	def create_payload(self):
		p = Payload()

		p.set_start(1)
		p.set_end(2)
		p.set_units(3)
		p.set_source(self.config['source'])
		p.set_id(self.config['name'])
		p.set_event(self.config['name'])
		p.set_event_source_timestamp(1234567890)
		p.set_payload('Community', [1, 2, 3, 4, 5, 6])
		p.set_payload('Community', [8, 9, 10, 12])
		p.set_payload('Units', [13409, 20934, 203942])

		f = open('results.txt', 'w')
		f.write(p.get_payload_data())
		f.close()

	def put(self):
		print('put')


sdk = Sdk()
sdk.create_payload()

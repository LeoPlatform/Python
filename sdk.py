#!/usr/local/bin/python3.6

import boto3
import json
from lib import dynamodb
from lib.payload import Payload


class Sdk:
	def __init__(self):
		self.config = {}
		self.session = None

		self.configuration()

	def configuration(self):
		# load the config file
		self.config = json.load(open('config.json'))

		# set the AWS profile (should be set in the config file)
		self.session = boto3.Session(profile_name=self.config['aws_profile'])

		print(self.session)
	# ddb = dynamodb(session)

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

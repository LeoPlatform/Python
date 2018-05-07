#import boto3

class dynamodb:
	def __init__(session):
		if (session):
			self.session = session

		self.instance = self.session.resource('dynamodb')

	#def get():
		
	#def update():
		
	#def put():
		
	#def delete():


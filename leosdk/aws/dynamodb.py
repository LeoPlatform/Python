# import boto3


class DynamoDB:
    def __init__(session):
        if (session):
            self.session = session

        self.instance = self.session.resource('DynamoDB')

# def get():

# def update():

# def put():

# def delete():

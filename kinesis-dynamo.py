mport base64
import json
##python lib for aws
import boto3
import datetime
import base64
import json
##python lib for aws
import boto3
import datetime


def lambda_handler(event, context):
    """
    Receive a batch of events from Kinesis and insert into  DynamoDB table
    
    """

    try: 
        ##resource assigned from boto library
        dynamo_db = boto3.resource('dynamodb')
        ##dynamoDB table name
        table = dynamo_db.Table('ip-streaming')
        
        for record in event["Records"]:
            decoded_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            print(decoded_data)
            print(json.loads(decoded_data))
            decoded_data_dic = json.loads(decoded_data)
            with table.batch_writer() as batch_writer:
                batch_writer.put_item(Item=decoded_data_dic)
       
    except Exception as e: 
        print(str(e))
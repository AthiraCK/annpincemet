import boto3
import json
import logging
from datetime import datetime
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodbTableName = 'announcementTable'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

getMethod = 'GET'
postMethod = 'POST'
announcementPath = '/announcement'
announcementsPath = '/announcements'
healthPath = '/health'

'''
The lambda handler will check the request type and will invoke the method for the same.
'''


def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        path = event['path']
        if http_method == getMethod and path == healthPath:
            return build_response(200)
        elif http_method == getMethod and path == announcementsPath:
            return get_announcements()
        elif http_method == postMethod and path == announcementPath:
            return save_announcement(event)
    except Exception as ex:
        logger.error(ex)


'''
This Function is used to fetch the announcement details from dynamodb
'''


def get_announcements():
    try:
        table_name = dynamodb.Table('AnnouncementDetails')
        response = table_name.scan()
        result = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], Limit=1)
            result.extend(response['Items'])
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as ex:
        logger.error(ex)


'''
This Function is used to save the announcement details to dynamodb
'''
def save_announcement(event):
    print('Loading function event', event)
    try:
        details_body = event['body']
        formatted_details = json.loads(details_body)
        print('formatted Body ', formatted_details)
        table_name = dynamodb.Table('AnnouncementDetails')
        event_date_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
        id = str(uuid.uuid4())
        formatted_details["id"] =id
        formatted_details["ann_created_dt"] = event_date_time
        table_name.put_item(Item=formatted_details)
        return {
            "statusCode": 200,
            "body": "successfully inserted the data"
        }
    except Exception as ex:
        logger.error(ex)

'''
This Function is used to check the server is up and running
'''
def build_response(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'body': "server is up and running",
        'isBase64Encoded': False
    }
    return response

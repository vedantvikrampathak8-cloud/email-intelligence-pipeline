import boto3, os, json
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')
s3       = boto3.client('s3')

TABLE  = os.environ['DYNAMO_TABLE']
BUCKET = os.environ['S3_BUCKET']
DAYS   = int(os.environ.get('RETENTION_DAYS', '90'))


def lambda_handler(event, context):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=DAYS)).isoformat()
    print(f"Deleting emails older than {cutoff}")

    table           = dynamodb.Table(TABLE)
    deleted_dynamo  = 0
    deleted_s3      = 0
    errors          = 0

    scan_kwargs = {
        'ProjectionExpression': 'email_id, received_at, s3_key',
        'FilterExpression': Attr('received_at').lt(cutoff)
    }

    while True:
        resp  = table.scan(**scan_kwargs)
        items = resp.get('Items', [])
        print(f"Found {len(items)} items to delete")

        for item in items:
            try:
                s3_key = item.get('s3_key', '')
                if s3_key:
                    s3.delete_object(Bucket=BUCKET, Key=s3_key)
                    deleted_s3 += 1
                table.delete_item(Key={'email_id': item['email_id']})
                deleted_dynamo += 1
            except Exception as e:
                print(f"Error: {str(e)}")
                errors += 1

        if 'LastEvaluatedKey' not in resp:
            break
        scan_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']

    summary = {'deleted_dynamo': deleted_dynamo, 'deleted_s3': deleted_s3, 'errors': errors, 'cutoff': cutoff}
    print(f"Done: {json.dumps(summary)}")
    return {'statusCode': 200, 'body': json.dumps(summary)}

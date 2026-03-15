import json, uuid, boto3, os
from datetime import datetime, timezone

REGION   = os.environ.get('AWS_REGION', 'us-east-1')
s3       = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
bedrock  = boto3.client('bedrock-runtime', region_name=REGION)

TABLE  = os.environ['DYNAMO_TABLE']
BUCKET = os.environ['S3_BUCKET']

CLASSIFY_PROMPT = """You are an email classifier. Analyze this email and respond ONLY with valid JSON, no other text, no markdown, no backticks.

Email:
From: {sender}
Subject: {subject}
Body: {body}

Classification rules:
- "important": Emails requiring action or attention — bills, job offers, personal messages, bank alerts, OTPs, shipping updates
- "sub-important": Informational emails from legitimate services — AWS notifications, account updates, newsletters you signed up for
- "advertisement": Promotional emails, deals, discounts, marketing from known companies
- "phishing": Genuinely malicious emails. Classify as phishing if ANY of these are true:
  1. Spelling mistakes in sender domain e.g. "amazzon.com", "paypa1.com", "g00gle.com"
  2. Legitimate brand name but suspicious domain e.g. "amazon-security.com", "apple-id-verify.net", "google-support.org"
  3. Free domain (gmail/yahoo/hotmail) pretending to be a bank or company
  4. Requests credentials, passwords, or OTP via a link
  5. Threatens account suspension/legal action to create urgency
  NEVER classify as phishing if sender domain is: google.com, amazon.com, amazon.in,
  reddit.com, github.com, kotak.com, hdfcbank.com, sbi.co.in, icicibank.com,
  axisbank.com, paytm.com, phonepe.com, nse.co.in, bse.co.in, zerodha.com,
  groww.in, marketplace.aws, amazonaws.com, classroom.google.com.
  Transaction alerts and OTPs from verified bank domains are "important".

Respond with exactly this JSON structure:
{{
  "classification": "<one of: important | sub-important | advertisement | phishing>",
  "summary": "<2-3 sentence summary>",
  "reason": "<one sentence explaining classification>",
  "urgency_score": <integer 1-10>
}}"""


def classify(sender, subject, body):
    prompt = CLASSIFY_PROMPT.format(
        sender=sender, subject=subject, body=body[:3000]
    )
    resp = bedrock.invoke_model(
        modelId='us.amazon.nova-micro-v1:0',
        body=json.dumps({
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {"max_new_tokens": 512}
        })
    )
    text = json.loads(resp['body'].read())['output']['message']['content'][0]['text']
    text = text.strip().removeprefix('```json').removeprefix('```').removesuffix('```').strip()
    return json.loads(text)


def embed(text):
    resp = bedrock.invoke_model(
        modelId='amazon.titan-embed-text-v2:0',
        body=json.dumps({"inputText": text[:8000]})
    )
    return json.loads(resp['body'].read())['embedding']


def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'content-type,x-api-secret',
            'Access-Control-Allow-Methods': 'POST,OPTIONS',
        }, 'body': ''}

    # Auth check
    secret = os.environ.get('API_SECRET', '')
    if event.get('headers', {}).get('x-api-secret') != secret:
        return {'statusCode': 401, 'body': 'Unauthorized'}

    body        = json.loads(event['body'])
    email_id    = str(uuid.uuid4())
    sender      = body.get('sender', '')
    subject     = body.get('subject', '')
    email_body  = body.get('body', '')
    received_at = body.get('received_at', datetime.now(timezone.utc).isoformat())

    result = classify(sender, subject, email_body)
    vector = embed(f"{subject}\n{email_body[:500]}")

    s3_key = f"emails/{result['classification']}/{email_id}.json"
    s3.put_object(
        Bucket=BUCKET, Key=s3_key,
        Body=json.dumps({
            'email_id': email_id, 'sender': sender,
            'subject': subject, 'body': email_body,
            'received_at': received_at
        }),
        ContentType='application/json'
    )

    dynamodb.Table(TABLE).put_item(Item={
        'email_id':       email_id,
        'sender':         sender,
        'subject':        subject,
        'received_at':    received_at,
        'classification': result['classification'],
        'summary':        result['summary'],
        'reason':         result['reason'],
        'urgency_score':  result['urgency_score'],
        's3_key':         s3_key,
        'embedding':      json.dumps(vector),
    })

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps({
            'email_id':       email_id,
            'classification': result['classification'],
            'summary':        result['summary'],
        })
    }

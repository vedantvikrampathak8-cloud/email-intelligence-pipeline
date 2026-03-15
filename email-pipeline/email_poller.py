import imaplib, email, urllib3, os, json, time
import boto3

REGION   = os.environ.get('AWS_REGION', 'us-east-1')
dynamodb = boto3.resource('dynamodb', region_name=REGION)
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

IMAP_SERVER    = 'imap.gmail.com'
EMAIL_ADDR     = os.environ['GMAIL_ADDRESS']
PASSWORD       = os.environ['GMAIL_APP_PASSWORD']
API_URL        = os.environ['API_URL']
API_SECRET     = os.environ['API_SECRET']
MSGID_TABLE    = os.environ.get('MSGID_TABLE', 'IngestedMessageIds')

http = urllib3.PoolManager()


def get_ingested_ids():
    """Fetch already-ingested message IDs from DynamoDB."""
    try:
        table  = dynamodb.Table(MSGID_TABLE)
        items  = []
        kwargs = {'ProjectionExpression': 'message_id'}
        while True:
            resp = table.scan(**kwargs)
            items.extend(resp.get('Items', []))
            if 'LastEvaluatedKey' not in resp:
                break
            kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        return set(i['message_id'] for i in items)
    except Exception as e:
        print(f"Could not fetch ingested IDs: {e}")
        return set()


def mark_ingested(message_id, ttl_days=90):
    """Store message ID so we never re-ingest it."""
    try:
        import time
        table = dynamodb.Table(MSGID_TABLE)
        table.put_item(Item={
            'message_id': message_id,
            'ttl': int(time.time()) + ttl_days * 86400
        })
    except Exception as e:
        print(f"Could not mark ingested: {e}")


def lambda_handler(event, context):

    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(EMAIL_ADDR, PASSWORD)
    mail.select('inbox')

    now        = datetime.now(timezone.utc)
    since_date = (now - timedelta(days=1)).strftime("%d-%b-%Y")
    print(f"Searching SINCE: {since_date}")

    _, data   = mail.search(None, f'(SINCE "{since_date}")')
    all_nums  = data[0].split() if data[0] else []
    print(f"Total emails since {since_date}: {len(all_nums)}")

    cutoff     = now - timedelta(hours=2)
    email_nums = all_nums[-20:]

    if not email_nums:
        print("No emails found")
        mail.logout()
        return {'statusCode': 200, 'body': 'No new emails'}

    # Fetch already-ingested message IDs
    ingested_ids = get_ingested_ids()
    print(f"Already ingested: {len(ingested_ids)} messages")

    ingested = []
    skipped  = 0

    for num in email_nums:
        try:
            _, msg_data = mail.fetch(num, '(RFC822)')
            msg         = email.message_from_bytes(msg_data[0][1])

            # Use Message-ID header as unique key
            message_id = msg.get('Message-ID', '').strip()
            if not message_id:
                # Fallback: use subject + date as key
                message_id = f"{msg.get('Subject','')}-{msg.get('Date','')}"

            # Skip if already ingested
            if message_id in ingested_ids:
                print(f"⏭ Already ingested: {msg.get('Subject', '')[:50]}")
                skipped += 1
                continue

            # Skip if too old
            date_str = msg.get('Date', '')
            try:
                email_dt = parsedate_to_datetime(date_str).astimezone(timezone.utc)
                if email_dt < cutoff:
                    print(f"⏭ Too old: {msg.get('Subject', '')[:50]}")
                    skipped += 1
                    continue
            except Exception:
                pass

            # Extract body
            body = ''
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        break
            else:
                body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')

            payload = {
                'sender':      msg.get('From', ''),
                'subject':     msg.get('Subject', ''),
                'body':        body[:4000],
                'received_at': date_str
            }

            resp   = http.request(
                'POST', API_URL,
                body=json.dumps(payload).encode('utf-8'),
                headers={'x-api-secret': API_SECRET, 'Content-Type': 'application/json'},
                timeout=30.0
            )
            result = json.loads(resp.data.decode('utf-8'))

            # Mark as ingested in DynamoDB
            mark_ingested(message_id)

            ingested.append({
                'subject':        payload['subject'],
                'classification': result.get('classification')
            })
            print(f"✓ {payload['subject'][:50]} → {result.get('classification')}")

        except Exception as e:
            print(f"✗ Failed: {str(e)}")
            continue

    mail.logout()
    print(f"Done — ingested {len(ingested)}, skipped {skipped}")
    return {
        'statusCode': 200,
        'body': json.dumps({'ingested': len(ingested), 'skipped': skipped, 'emails': ingested})
    }

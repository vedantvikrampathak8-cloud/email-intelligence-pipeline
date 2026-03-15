import imaplib, email, urllib3, json
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

IMAP_SERVER = 'imap.gmail.com'
EMAIL_ADDR  = 'your-gmail@gmail.com'       # ← change this
PASSWORD    = 'your-app-password'           # ← change this
API_URL     = 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/ingest-email'  # ← change this
API_SECRET  = 'your-api-secret'            # ← change this
DAYS_BACK   = 30

http   = urllib3.PoolManager()
mail   = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_ADDR, PASSWORD)
mail.select('inbox')

now        = datetime.now(timezone.utc)
cutoff     = now - timedelta(days=DAYS_BACK)
since_date = cutoff.strftime("%d-%b-%Y")

print(f"Fetching emails since {since_date}...")
_, data  = mail.search(None, f'(SINCE "{since_date}")')
all_nums = data[0].split() if data[0] else []
print(f"Found {len(all_nums)} emails")

ingested = skipped = failed = 0

for i, num in enumerate(all_nums):
    try:
        _, msg_data = mail.fetch(num, '(RFC822)')
        msg         = email.message_from_bytes(msg_data[0][1])
        date_str    = msg.get('Date', '')
        try:
            email_dt = parsedate_to_datetime(date_str).astimezone(timezone.utc)
            if email_dt < cutoff:
                skipped += 1
                continue
        except Exception:
            pass

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
        ingested += 1
        if ingested % 10 == 0:
            print(f"Progress: {ingested} ingested, {i+1}/{len(all_nums)} processed")
        print(f"✓ [{result.get('classification')}] {payload['subject'][:60]}")

    except Exception as e:
        failed += 1
        print(f"✗ Failed: {str(e)}")
        continue

mail.logout()
print(f"\nDone — {ingested} ingested, {skipped} skipped, {failed} failed")

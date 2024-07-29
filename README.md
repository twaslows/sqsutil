Installation:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
touch sqs_messages.jsonl
```

Usage:
    
```bash
python main.py <purge | receive>
```
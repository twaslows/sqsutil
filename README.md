Installation:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
touch alerting.jsonl
echo <QUEUE_URL> > .envrc && direnv allow
# or export QUEUE_URL=<QUEUE_URL>
```

Usage:

```bash
python main.py <purge | receive>
```

import datetime
import json
import os
import time

import boto3
import click
from click import secho

"""
Script for creating and analyzing SQS messages.
"""

QUEUE_URL = os.environ["QUEUE_URL"]
OUT_FILE = "sqs_messages.jsonl"
POLLING_FREQUENCY = 5

client = boto3.client("sqs", region_name="eu-central-1")


@click.group()
def cli():
    pass


@click.command()
def receive():
    while True:
        messages = client.receive_message(
            QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, VisibilityTimeout=1200
        ).get("Messages", [])
        if not messages:
            secho(f"{datetime.datetime.now().isoformat()}: No messages found")
            time.sleep(POLLING_FREQUENCY)
            continue
        else:
            secho(f"Found {len(messages)} messages")
            for message in messages:
                sns_message = json.loads(message["Body"])["Message"]
                alerts = json.loads(sns_message)["alerts"]
                for alert in alerts:
                    secho(
                        f"{alert['startsAt'].split('+')[0].strip().replace(' ', 'T')} - {alert['labels']['service']} - {alert['labels']['alertname']}"
                    )
                with open(OUT_FILE, "a") as f:
                    f.write(json.dumps(alerts) + "\n")
                client.delete_message(
                    QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                )
                secho("Message deleted")
        secho(f"Waiting {POLLING_FREQUENCY} seconds")
        time.sleep(POLLING_FREQUENCY)


@click.command()
def purge():
    client.purge_queue(QueueUrl=QUEUE_URL)
    secho("Queue purged")


cli.add_command(purge)
cli.add_command(receive)

if __name__ == "__main__":
    cli()

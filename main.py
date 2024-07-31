import boto3
import click
import datetime
import json
import os
import time
from click import secho

"""
Script for creating and analyzing SQS messages.
"""

QUEUE_URL = os.environ.get("QUEUE_URL")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
OUT_FILE = "sqs_messages.jsonl"
POLLING_FREQUENCY = 5

sqs_client = boto3.client("sqs", region_name="eu-central-1")
sns_client = boto3.client("sns", region_name="eu-central-1")


@click.group()
def cli():
    pass


@click.command()
@click.option("--delete", is_flag=True, default=True, help="The URL of the SQS queue")
@click.option("--full-message", is_flag=True, default=False, help="Store the full received SNS message")
@click.option("--queue-url", default=QUEUE_URL, help="Queue URL; can be overwritten by environment variable")
def receive(delete, full_message, queue_url):
    while True:
        secho(f"{datetime.datetime.now().isoformat()}: Polling for messages on Queue {queue_url}")
        messages = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, VisibilityTimeout=1200
        ).get("Messages", [])
        if not messages:
            secho(f"{datetime.datetime.now().isoformat()}: No messages found")
            time.sleep(POLLING_FREQUENCY)
            continue
        else:
            secho(f"Found {len(messages)} messages")
            for message in messages:
                body = json.loads(message["Body"])["Message"]
                alerts = json.loads(body)["alerts"]
                for alert in alerts:
                    secho(
                        f"{alert['startsAt'].split('+')[0].strip().replace(' ', 'T')} - {alert['labels']['service']} - {alert['labels']['alertname']}"
                    )
                with open(OUT_FILE, "a") as f:
                    if full_message:
                        f.write(json.dumps(message) + "\n")
                    else:
                        f.write(json.dumps(alerts) + "\n")
                if delete:
                    sqs_client.delete_message(
                        QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                    )
                secho("Message deleted")
        secho(f"Waiting {POLLING_FREQUENCY} seconds")
        time.sleep(POLLING_FREQUENCY)


@click.command()
@click.option("--event", required=True, help="Message to publish; reads from a JSON file")
def publish(event):
    with open(event, "r") as f:
        message = json.load(f)
    sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=json.dumps(message))
    secho("Message published")


@click.command()
def purge():
    sqs_client.purge_queue(QueueUrl=QUEUE_URL)
    secho(f"Queue {QUEUE_URL} purged")


cli.add_command(purge)
cli.add_command(receive)
cli.add_command(publish)

if __name__ == "__main__":
    cli()

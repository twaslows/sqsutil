import datetime
import json
import os
import time

import boto3
import click
from click import secho
from sqs_util.util import *

"""
Script for creating and analyzing SQS messages.
"""

QUEUE_URL = os.environ.get("QUEUE_URL")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
POLLING_FREQUENCY = 5

sqs_client = boto3.client("sqs", region_name="eu-central-1")
sns_client = boto3.client("sns", region_name="eu-central-1")


@click.group()
@click.option("--debug", default=False)
def cli(debug: bool):
    pass


@click.command()
@click.option("--delete", is_flag=True, default=False, help="Delete polled messages from the Queue")
@click.option(
    "--full-message",
    is_flag=True,
    default=False,
    help="Store the full received SNS message instead of just its data block",
)
@click.option(
    "--queue-url",
    default=QUEUE_URL,
    help="Queue URL; defaults to the QUEUE_URL environment variable",
)
@click.option("--out-file", help="Output file")
def receive(delete: bool, full_message: bool, queue_url: str, out_file: str):
    while True:
        debug(
            f"{datetime.datetime.now().isoformat()}: Polling for messages on Queue {queue_url}"
        )
        # Poll for messages
        messages = sqs_client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, VisibilityTimeout=1200
        ).get("Messages", [])
        if not messages:
            info(f"{datetime.datetime.now().isoformat()}: No messages found")
            time.sleep(POLLING_FREQUENCY)
            continue

        else:
            secho(f"Found {len(messages)} messages")
            for message in messages:
                body = json.loads(message["Body"])["Message"]
                if not out_file:
                    out_file = f"out/{queue_url}_events.jsonl"
                with open(f"out/{out_file}", "a") as f:
                    if full_message:
                        f.write(json.dumps(message) + "\n")
                    else:
                        f.write(json.dumps(body) + "\n")
                if delete:
                    sqs_client.delete_message(
                        QueueUrl=QUEUE_URL, ReceiptHandle=message["ReceiptHandle"]
                    )
                secho("Message deleted")
        secho(f"Waiting {POLLING_FREQUENCY} seconds")
        time.sleep(POLLING_FREQUENCY)


@click.command()
@click.option(
    "--event", required=True, help="Message to publish; reads from a JSON file"
)
@click.option(
    "--topic-arn",
    default=SNS_TOPIC_ARN,
    help="SNS Topic ARN; defaults to the SNS_TOPIC_ARN environment variable",
)
def publish(event, topic_arn):
    """Publish to a specified topic"""
    with open(f"events/{event}", "r") as f:
        message = json.load(f)
    sns_client.publish(TopicArn=topic_arn, Message=json.dumps(message))
    secho("Message published")


@click.command()
@click.option("--queue-url", default=QUEUE_URL, help="Queue URL; defaults to the QUEUE_URL environment variable")
def purge():
    sqs_client.purge_queue(QueueUrl=QUEUE_URL)
    secho(f"Queue {QUEUE_URL} purged")


cli.add_command(purge)
cli.add_command(receive)
cli.add_command(publish)

if __name__ == "__main__":
    cli()

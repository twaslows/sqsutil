import datetime
import json
import time

from click import secho

from sqs_util.util import BaseClient, click, create_client, debug, info

"""
Script for creating and analyzing SQS messages.
"""

_global_options = [
    click.option(
        "--local", "-l", is_flag=True, default=False, help="Use localstack endpoint"
    ),
    click.option(
        "--debug", "-d", "_debug", is_flag=True, default=False, help="Print debug logs"
    ),
]


def global_opts(func):
    for option in reversed(_global_options):
        func = option(func)
    return func


@click.group()
def cli():
    pass


@cli.command()
@click.argument("queue-url")
@click.option(
    "--delete",
    is_flag=True,
    default=False,
    help="Delete polled messages from the Queue",
)
@click.option("--out-file", help="Output file", type=click.Path(exists=True))
@click.option(
    "--polling-frequency", envvar="POLLING_FREQUENCY", help="Polling frequency"
)
@global_opts
def receive(
    delete: bool,
    queue_url: str,
    out_file: str,
    local: bool,
    _debug: bool,
    polling_frequency: int,
):
    sqs_client = create_client("sqs", local)
    while True:
        messages = poll(queue_url, sqs_client)
        if not messages:
            info(f"{datetime.datetime.now().isoformat()}: No messages found")
            time.sleep(polling_frequency)
            continue
        else:
            secho(f"Found {len(messages)} messages")
            for message in messages:
                process_message(delete, message, out_file, queue_url, sqs_client)
                secho("Message deleted")
        secho(f"Waiting {polling_frequency} seconds")
        time.sleep(polling_frequency)


def process_message(
    delete: bool,
    message: dict,
    out_file: str,
    queue_url: str,
    sqs_client: BaseClient,
) -> None:
    body = json.loads(message["Body"])["Message"]
    if not out_file:
        out_file = f"out/{queue_url}_events.jsonl"
    with open(f"out/{out_file}", "a") as f:
        f.write(json.dumps(body) + "\n")
    if delete:
        sqs_client.delete_message(
            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
        )


def poll(queue_url: str, sqs_client: BaseClient) -> list[dict]:
    debug(
        f"{datetime.datetime.now().isoformat()}: Polling for messages on Queue {queue_url}"
    )
    return sqs_client.receive_message(
        QueueUrl=queue_url, MaxNumberOfMessages=10, VisibilityTimeout=1200
    ).get("Messages", [])


@cli.command()
@global_opts
def list_queues(local: bool, _debug: bool):
    """List all queues"""
    sqs_client = create_client("sqs", local)
    queues = sqs_client.list_queues()
    for queue in queues["QueueUrls"]:
        secho(queue)


@cli.command()
@click.option(
    "--event",
    required=True,
    help="Message to publish; reads from a JSON file",
    type=click.Path(exists=True),
)
@click.option(
    "--topic-arn",
    envvar="SNS_TOPIC_ARN",
    help="SNS Topic ARN; defaults to the SNS_TOPIC_ARN environment variable",
)
@global_opts
def publish(local: bool, event: str, topic_arn: str, _debug: bool):
    """Publish to a specified topic"""
    sns_client = create_client("sns", local)
    with open(f"{event}", "r") as f:
        payload = json.load(f)
        message = payload.get("Message")
        message_attributes = payload.get("MessageAttributes")
        debug(f"Message: {message}")
    sns_client.publish(TopicArn=topic_arn, Message=json.dumps(message), MessageAttributes=message_attributes)
    secho("Message published")


@cli.command()
@click.argument("queue-url")
@global_opts
def purge(queue_url: str, _debug: bool, local: bool):
    """Purge a queue"""
    sqs_client = create_client("sqs", local)
    sqs_client.purge_queue(QueueUrl=queue_url)
    secho(f"Queue {queue_url} purged")


if __name__ == "__main__":
    cli()

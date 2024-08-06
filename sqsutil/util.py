import boto3
import click
from botocore.client import BaseClient


def warn(message):
    click.secho(str(message), fg="yellow")


@click.pass_context
def error(ctx, message):
    click.secho(str(message), fg="red")
    ctx.exit(1)


def info(message):
    click.secho(str(message))


@click.pass_context
def debug(ctx: click.Context, message):
    if ctx.params.get("debug"):
        click.secho(str(message), fg="blue")


def determine_endpoint(local: bool) -> str:
    return "http://localhost:4566" if local else None


def create_client(service: str, local: bool) -> BaseClient:
    endpoint = determine_endpoint(local)
    return boto3.client(service, region_name="eu-central-1", endpoint_url=endpoint)

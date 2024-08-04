import click


@click.pass_context
def warn(ctx, message):
    click.secho(str(message), fg="yellow")


@click.pass_context
def error(ctx, message):
    click.secho(str(message), fg="red")
    ctx.exit(1)


@click.pass_context
def info(ctx, message):
    click.secho(str(message))


@click.pass_context
def debug(ctx: click.Context, message):
    if ctx.params.get("debug"):
        click.secho(str(message), fg="blue")

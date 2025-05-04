import sys
import click
import json


def error_print(command_name, error):
    click.echo(f"Error during {command_name}: \n{str(error)}", err=True)
    sys.exit(1)


def verbose_print(message):
    ctx = click.get_current_context()
    if ctx.obj["verbose"]:
        click.echo(message)


def json_print(data):
    click.echo(json.dumps(data, indent=2))

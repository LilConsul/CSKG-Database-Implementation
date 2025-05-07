import sys
import click
import json


def error_print(command_name, error):
    click.echo(f"Error during execution of '{command_name}': \n{str(error)}", err=True)
    sys.exit(1)


def verbose_print(message):
    ctx = click.get_current_context()
    if ctx.obj["verbose"]:
        click.echo(message)


def json_print(data):
    click.echo(json.dumps(data, indent=2))


def length_print(function_name: str, results: dict):
    result_list = results.get(list(results.keys())[0], []) if results else []
    count = len(result_list)
    element_text = "element" if count == 1 else "elements"
    verbose_print(f"{function_name} has {count} {element_text}")

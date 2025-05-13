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
    ctx = click.get_current_context()
    try:
        if ctx.obj.get("raw", False):
            if (
                isinstance(data, list)
                and data
                and isinstance(data[0], dict)
                and "label" in data[0]
            ):
                labels = [node["label"] for node in data]
                click.echo(", ".join(labels))
                return
            elif isinstance(data, dict):
                result = data.get(list(data.keys())[0], []) if data else []
                if result and isinstance(result[0], dict) and "label" in result[0]:
                    result = [item["label"] for item in result]
                    click.echo(", ".join(result))
                elif result and isinstance(result[0], dict) and "count" in result[0]:
                    click.echo(result[0]["count"])
                else:
                    click.echo(result)
                return
            click.echo(data)
            return
        else:
            click.echo(json.dumps(data, indent=2))
    except Exception:
        click.echo(json.dumps(data, indent=2))


def length_print(function_name: str, results: dict):
    result_list = results.get(list(results.keys())[0], []) if results else []
    count = len(result_list)
    element_text = "element" if count == 1 else "elements"
    verbose_print(f"{function_name} has {count} {element_text}")

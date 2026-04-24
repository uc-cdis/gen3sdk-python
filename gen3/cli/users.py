import click

from gen3users import main as users_cli
import sys

from gen3.utils import load_entry_points

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points


@click.group()
def main():
    """Gen3 Command Line Interface"""
    pass


@click.group()
def users():
    """Commands for working with gen3users"""
    pass


for command in users_cli.main.commands:
    users.add_command(users_cli.main.get_command(ctx=None, cmd_name=command))

load_entry_points()

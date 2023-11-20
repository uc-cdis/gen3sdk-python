import click

from gen3users import main as users_cli


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

# load plug-ins from entry_points
for ep in entry_points().get("gen3.plugins", []):
    ep.load()

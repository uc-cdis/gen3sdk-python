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
try:
    eps = entry_points()
    if hasattr(eps, 'get'):
        # Python < 3.10 and importlib_metadata < 3.6
        gen3_plugins = eps.get("gen3.plugins", [])
    else:
        # Python >= 3.10 and importlib_metadata >= 3.6
        gen3_plugins = eps.select(group="gen3.plugins")
    
    for ep in gen3_plugins:
        ep.load()
except Exception:
    # If entry_points fails, just continue without plugins
    pass

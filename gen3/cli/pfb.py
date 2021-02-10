import click
from pfb import cli as pfb_cli

try:
    from importlib.metadata import entry_points
except ImportError:
    from importlib_metadata import entry_points


@click.group()
def main():
    """Gen3 Command Line Interface"""
    pass


@click.group()
def pfb():
    """Commands for working with Portable Format for Biomedical Data (PFB)"""
    pass


for command in pfb_cli.main.commands:
    pfb.add_command(pfb_cli.main.get_command(ctx=None, cmd_name=command))

# load plug-ins from entry_points
for ep in entry_points().get("gen3.plugins", []):
    ep.load()

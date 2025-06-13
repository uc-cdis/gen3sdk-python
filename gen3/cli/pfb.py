import click
from importlib.metadata import entry_points
from pfb import cli as pfb_cli


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

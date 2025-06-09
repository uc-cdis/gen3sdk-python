import click

try:
    from pypfb import cli as pfb_cli
    PYPFB_AVAILABLE = True
except ImportError:
    PYPFB_AVAILABLE = False
    pfb_cli = None

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
    if not PYPFB_AVAILABLE:
        click.echo("Error: pypfb is not installed. Install with: pip install 'pypfb[gen3]'")
        return


if PYPFB_AVAILABLE and pfb_cli:
    for command in pfb_cli.main.commands:
        pfb.add_command(pfb_cli.main.get_command(ctx=None, cmd_name=command))

# load plug-ins from entry_points
try:
    eps = entry_points()
    if hasattr(eps, 'get'):
        # Older style
        gen3_plugins = eps.get("gen3.plugins", [])
    else:
        # Newer style
        gen3_plugins = eps.select(group="gen3.plugins")
    
    for ep in gen3_plugins:
        ep.load()
except Exception:
    # If entry_points fails, just continue without plugins
    pass

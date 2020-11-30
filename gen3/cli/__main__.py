import click
import os
import gen3.cli.auth as auth
import gen3.cli.pfb as pfb
import gen3.cli.wss as wss
import gen3


class AuthFactory:
    def __init__(self, refresh_file):
        self.refresh_file = refresh_file
        self._cache = None

    def get(self):
        """Lazy factory"""
        if self._cache:
            return self._cache
        self._cache = gen3.auth.Gen3Auth(refresh_file=self.refresh_file)
        return self._cache


@click.group()
@click.option("--auth", 'auth_config', default=os.getenv("GEN3_API_KEY", None), help="path to api key, or shorthand for key in ~/.gen3/, or idp: identifier if in workspace")
@click.option("--endpoint", 'endpoint', default=os.getenv("GEN3_ENDPOINT", "default"), help="commons hostname - optional if API Key given")
@click.pass_context
def main(ctx=None, auth_config=None, endpoint=None):
    """Gen3 sdk commands"""
    ctx.ensure_object(dict)
    ctx.obj["auth_config"] = auth_config
    ctx.obj["endpoint"] = endpoint
    ctx.obj["auth_factory"] = AuthFactory(auth_config)

main.add_command(auth.auth)
main.add_command(pfb.pfb)
main.add_command(wss.wss)

main()

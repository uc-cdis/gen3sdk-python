"""
Contains common behaviors for HTTP interactions like disabling TLS verification
as needed (e.g. on localhost) and to abstract away from disparate HTTP clients.
"""
import aiohttp
import requests

__verify = True


def set_ssl_verify(value):
    global __verify
    __verify = value
    print("updated", __verify)


def get_ssl_verify():
    global __verify
    return __verify


class AiohttpWrapper:
    """
    A class for generically handling TLS verification for aiohttp requests.

    Examples:
        This allocates an AiohttpWrapper class with verification on or off.

        >>> wrapper = AiohttpWrapper(False)
    """

    def __init__(self, verify):
        """
        Initialization for instance of the class to set up the wrapped client.
        """
        self.client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=verify)
        )

    async def __aenter__(self):
        """
        Return the allocated HTTP client for an async "with" context.
        """
        return self.client

    async def __aexit__(self, *err):
        """
        Close the client context when leaving the wrapped "with" context.
        """
        await self.client.close()
        self.client = None


def http_client(library):
    if library == "requests":
        session = requests.Session()
        session.verify = __verify
        print("session:", __verify)
        return session
    elif library == "aiohttp":
        print("aiohttp:", __verify)
        return AiohttpWrapper(__verify)

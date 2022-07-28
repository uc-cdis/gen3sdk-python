import requests
from gen3.utils import raise_for_status_and_print_error


class Gen3ObjectError(Exception):
    pass


class Gen3Object:
    """For interacting with Gen3 object level features.

    A class for interacting with the Gen3 object services.
    Currently allows creating and deleting of an object from the Gen3 System.

    Args:
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Object class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... object = Gen3Object(auth)

    """

    def __init__(self, auth_provider=None):
        self._auth_provider = auth_provider
        self.service_endpoint = "/mds"

    def create_object(self, file_name, authz, metadata=None, aliases=None):
        url = (
            self._auth_provider.endpoint.rstrip("/")
            + self.service_endpoint
            + "/objects"
        )
        body = {
            "file_name": file_name,
            "authz": authz,
            "metadata": metadata,
            "aliases": aliases,
        }
        response = requests.post(url, json=body, auth=self._auth_provider)
        raise_for_status_and_print_error(response)
        data = response.json()
        return data["guid"], data["upload_url"]

    def delete_object(self, guid, delete_file_locations=False):
        """
        Delete the object from indexd, metadata service and optionally all storage locations

        Args:
            `guid`  -- GUID of the object to delete
            `delete_file_locations` -- if True, removes the object from existing bucket location(s) through fence
        Returns:
            Nothing
        """
        delete_param = "?delete_file_locations" if delete_file_locations else ""
        url = (
            self._auth_provider.endpoint.rstrip("/")
            + self.service_endpoint
            + "/objects/"
            + guid
            + delete_param
        )
        response = requests.delete(url, auth=self._auth_provider)
        raise_for_status_and_print_error(response)

import logging
import requests
from gen3 import file, metadata, auth as auth_tool, index as indexd
from gen3.utils import raise_for_status


class Gen3ObjectError(Exception):
    pass


class Gen3Object:
    """For interacting with Gen3 object level features.

    A class for interacting with the Gen3 object services.
    Currently allows deleting of an object from the Gen3 System.

    Args:
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Object class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... file = Gen3File(auth)

    """

    def __init__(self, auth_provider=None):
        self._auth_provider = auth_provider

    def create_object(self, file_name, authz, metadata=None, aliases=None):
        url = self.endpoint + "/objects"
        body = {
            "file_name": file_name,
            "authz": authz,
            "metadata": metadata,
            "aliases": aliases,
        }
        response = requests.post(url, json=body, auth=self._auth_provider)
        raise_for_status(response)
        data = response.json()
        return data["guid"], data["upload_url"]

    def delete_object(self, guid, delete_record=False):
        """
        Delete the object from indexd, metadata service and optionally all storage locations

        Args:
            guid (str): provide a UUID for file id to delete
            delete_record (boolean) : [Optional] If True, delete the record from all storage locations
        Returns:
            Nothing
        """
        meta = metadata.Gen3Metadata(auth_provider=self._auth_provider)
        metadata_response_object = meta.query(f"guid={guid}&data=True", True)

        if metadata_response_object:
            try:
                metadata_response_object = meta.delete(guid)
                logging.info(metadata_response_object)
            except Exception as exp:
                raise Exception(
                    f"Error in deleting object with {guid} from Metadata Service. Exception -- {exp}"
                )

        try:
            if delete_record:
                fence = file.Gen3File(auth_provider=self._auth_provider)
                response = fence.delete_file_locations(guid)
                if response.status_code != 204:
                    raise Exception(
                        f"Error in deleting object with {guid} from Fence. Response -- {response}"
                    )
                logging.info("Deleted files succesfully")
            else:
                index = indexd.Gen3Index(auth_provider=self._auth_provider)
                response = index.delete_record(guid)
                logging.info("Deleted records succesfully")
        except Exception as exp:
            if metadata_response_object:
                meta.create(guid, metadata_response_object)
            raise

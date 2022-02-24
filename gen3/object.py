import logging
from gen3 import file, metadata, auth as auth_tool, index as indexd


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

    def __init__(self, endpoint=None, auth_provider=None):
        # auth_provider legacy interface required endpoint as 1st arg
        self._auth_provider = auth_provider or endpoint
        self._endpoint = self._auth_provider.endpoint

    def delete_object(self, guid, delete_record=False):
        """
        Delete the all the records of the guid from all storage locations, indexd and metadata service

        Args:
            guid (str): provide a UUID for file id to delete
        Returns:
            text: requests.delete text result
        """
        fence = file.Gen3File(auth_provider=self._auth_provider)
        meta = metadata.Gen3Metadata(auth_provider=self._auth_provider)
        metadata_response_object = None
        metadata_response_object = meta.query(f"guid={guid}&data=True", True)

        if metadata_response_object:
            try:
                metadata_response_object = meta.delete(guid)
                logging.info(metadata_response_object)
            except Exception as exp:
                return f"Error in deleting object with {guid} from Metadata Service. Exception -- {exp}"

        index = indexd.Gen3Index(auth_provider=self._auth_provider)
        idx_record = index.get(guid)

        if not idx_record:
            return f"No record found with guid- {guid}"

        if delete_record:
            response = fence.delete_file_locations(guid)
            if response.status_code != 204:
                if metadata_response_object:
                    meta.create(guid, metadata_response_object)
                return f"Error in deleting object with {guid} from Fence. Response -- {response}"
            return "Deleted files succesfully"
        else:
            response = index.delete_record(guid)
            return "Deleted records succesfully"


if __name__ == "__main__":
    auth = auth_tool.Gen3Auth(
        refresh_file="/Users/saishanmukhanarumanchi/Documents/git_projects/playground/sai_creds.json"
    )
    gen3Obj = Gen3Object(auth_provider=auth)
    fence = file.Gen3File(auth)
    guid = fence.upload_file("query.py", protocol="s3")["guid"]
    print(guid)
    print(gen3Obj.delete_object(guid, delete_record=True))

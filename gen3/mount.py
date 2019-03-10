import requests
import subprocess
from datetime import datetime
import urllib 
import os


class Gen3MountError(Exception):
    pass

class Gen3Mount:
    """Mount cohort data from a commons for local analysis.

    A class to facilitate locally mounting data from a manifest file.
    For local use, provide an api_key argument. For use in a Jupyter notebook hosted by the same commons, this parameter is not required. 
    Can communicate with the manifest service, the workspace token service, gen3-fuse, and fence.

    Args:
        endpoint (str): The URL of the data commons.
        api_key (str): The user's api key (optional).
        config_yaml_path (str): The path to a Gen3Fuse config file (optional). Defaults to "/home/jovyan/fuse-config.yaml".
        wts_url (str): The path to the workspace token service (optional). Defaults to "http://workspace-token-service".

    Examples:
        From within a Jupyter notebook, these commands will mount the user's most recent data export for local analysis.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        >>> g3mount = Gen3Mount(endpoint)
        >>> g3mount.mount_my_last_export()

        On a user's local machine, these commands achieve the same result.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        >>> api_key = "xxx" # Fill this value in with a generated key from your profile page
        >>> g3mount = Gen3Mount(endpoint, api_key=api_key)
        >>> g3mount.mount_my_last_export()
    """

    def __init__(self, endpoint, api_key=None, config_yaml_path="/home/jovyan/fuse-config.yaml", wts_url="http://workspace-token-service"):
        self.endpoint = endpoint
        self.api_key = api_key
        self.access_token = ""
        self.wts_url = wts_url
        self.config_yaml_path = config_yaml_path

    def _update_access_token(self):
        if self.api_key is not None:
        	self.access_token = self._get_access_token_from_fence()
        else:
            self.access_token = self._get_access_token_from_wts()

    def _get_access_token_from_fence(self):
        """
        Communicate with fence to trade the user's api key for an access token. 
        """
        data = {
            'api_key' : self.api_key
        }
        headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
        fence_token_url = self.endpoint + "/user/credentials/api/access_token"
        try:
            r = requests.post(fence_token_url, json=data, headers=headers)
            token = r.json()['access_token']
            return token
        except Exception as e:
            raise Gen3MountError("Failed to authenticate with {}\n{}\n{}".format(fence_token_url, r.text, str(e)))

    def _get_access_token_from_wts(self):
        """
        Communicate with fence to trade the user's api key for an access token. 
        Updates the access_token instance variable. 

        Args:
            api_key (str): The user's api key, generated on their identity page.
        """ 
        try:
            r = requests.get(self.wts_url + "/token")
            token = r.json()["token"]
            return token
        except Exception as e:
            raise Gen3MountError("Failed to authenticate to {}\n{}".format(self.wts_url, str(e)))

    def mount_my_last_export(self):
        """
        Mount the manifest corresponding to the most recent export by this user.
        The result of this command will be a new directory named mount-pt-<timestamp> which contains
        files corresponding to records exported from the explorer. 
        Returns the output from the Gen3Fuse mount command.
        """
        manifests = self.list_manifests()
        manifests_sorted_by_last_modified_date = sorted(manifests, key=lambda k: k['last_modified'])
        most_recent_filename = manifests_sorted_by_last_modified_date[-1]['filename']
        manifest_filepath = self._download_manifest(most_recent_filename)
        return self.mount_manifest(manifest_filepath)

    def _download_manifest(self, filename):
        """
        Retrieves and saves the requested manifest file locally.
        This function communicates with the manifest service to download the file from the user's manifest folder.

        Args:
            filename (str): The name of the manifest file to download, of the form "manifest-timestamp.json".

        Returns the local filepath to the saved manifest.
        """
        if not filename.startswith("manifest-") :
            raise Gen3MountError("The filename argument must be a manifest file beginning with 'manifest-'.")
            return
        
        self._update_access_token()
        headers = {'Content-Type': 'application/json', 'Accept':'application/json', 'Authorization' : "bearer " + self.access_token }
        request_url = self.endpoint + "manifests/file/" + urllib.parse.quote_plus(filename)

        try:
            r = requests.get(request_url, headers=headers)
            manifest_body = r.json()['body']
        except Exception as e:
            raise Gen3MountError("Failed to parse manifest service response at {}\n{}\n{}".format(request_url, r.text, str(e)))
        
        manifests_directory = "my_manifests/"
        if not os.path.exists(manifests_directory):
            os.makedirs(manifests_directory)
        filepath = manifests_directory + filename
        f = open(filepath , 'w')
        f.write(manifest_body)
        return filepath

    def mount_manifest(self, filename):
        """
        Mount the manifest corresponding to the filename argument.
        The result of this command will be a new directory named mount-pt-<timestamp> which contains
        files corresponding to records exported from the explorer.
        On success, returns the path to the mounted files, otherwise returns None.
        """
        directory_path = "mount-pt-" + datetime.now().isoformat().replace(":", "-")
        if self.api_key:
            command = "gen3fuse {} {} {} {} {}".format(self.config_yaml_path, filename, directory_path, self.endpoint, self.api_key)
        else:
            command = "gen3fuse {} {} {} {}".format(self.config_yaml_path, filename, directory_path, self.endpoint)

        output = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True).stdout.read()
        if output == '':
            print("Your exported files have been mounted to {}/".format(directory_path))
            return directory_path
        
        print("Gen3Fuse error: " + output)
        return

    def list_manifests(self):
        """
        List the filenames and last_modified times of the files in the user's manifest folder. 
        This function communicates with the manifest service. 
        It returns a list of the form [ {'filename' : 'manifest-timestamp.json', 'last_modified' : 'modified-datetime' } , ... ]
        """
        self._update_access_token()
        headers = {'Content-Type': 'application/json', 'Accept':'application/json', 'Authorization' : "bearer " + self.access_token }
        list_manifest_url = self.endpoint + 'manifests/'

        try:
            r = requests.get(list_manifest_url, headers=headers)
            manifest_files = r.json()['manifests']
            return manifest_files
        except Exception as e:
            raise Gen3MountError("Failed to parse manifest service response {}\n{}\n{}".format(list_manifest_url, r.text, str(e)))
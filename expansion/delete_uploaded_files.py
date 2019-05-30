import requests, json

class Gen3Delete:

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint


    def get_token(self,creds,api):
        with open (creds, 'r') as f:
            credentials = json.load(f)
        token_url = api + '/user/credentials/api/access_token'
        resp = requests.post(token_url, json=credentials)
        if (resp.status_code != 200):
            raise(Exception(resp.reason))
        token = resp.json()['access_token']
        return token


    def delete_uploaded_file(self,guid):
        headers = {'Authorization': 'bearer ' + get_token()}

        fence_url = api + '/user/data/'
        response = requests.delete(
            fence_url + guid,
            headers=headers
        )
        if (response.status_code == 204):
            print("Successfully deleted GUID {}".format(guid))
        else:
            print("Error deleting GUID {}:".format(guid))
            print(response.reason)

    def clear_log(self,logfile):
        """Returns a list of data GUIDs from a logfile.
        """
        with open(logfile) as json_file:
            data = json.load(json_file)
        guids = list(data.values())
        for guid in guids:
            delete_uploaded_file(guid,api)


    # if __name__ == "__main__":
    #     #guid = "be022658-06a2-4738-a58f-765804ab2254"
    #     creds = '/Users/christopher/Downloads/vpodc-credentials.json'
    #     api = 'https://vpodc.org/'
    #     logfile = '/Users/christopher/.gen3/OLD_vpodc_succeeded_log.json'
    #     clear_log(logfile,api)

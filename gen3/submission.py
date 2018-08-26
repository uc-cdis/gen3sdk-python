import json
import requests


class Gen3SubmissionQueryError(Exception):
    pass


class Gen3Submission:
    """
    A class for interacting with the Gen3 submission services.
    Supports submitting and exporting from Sheepdog.
    Supports GraphQL queries through Peregrine.
    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint

    def query(self, query_txt, variables=None, max_tries=1):
        api_url = "{}/api/v0/submission/graphql".format(self._endpoint)
        if variables == None:
            query = {"query": query_txt}
        else:
            query = {"query": query_txt, "variables": variables}

        tries = 0
        while tries < max_tries:
            output = requests.post(api_url, auth=self._auth_provider, json=query).text
            data = json.loads(output)

            if "errors" in data:
                raise Gen3SubmissionQueryError(data["errors"])

            if not "data" in data:
                print(query_txt)
                print(data)

            tries += 1

        return data

    def export(self, program, project, uuid):
        api_url = "{}/api/v0/submission/{}/{}/export?ids={}&format=json".format(
            self._endpoint, program, project, uuid
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        data = json.loads(output)
        return data

    def submit(self, program, project, json):
        api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
        output = requests.put(api_url, auth=self._auth_provider, json=json).text
        return output

import json
import requests


class Gen3SubmissionQueryError(Exception):
    pass


class Gen3Submission:
    """Submit/Export/Query data from a Gen3 Submission system.

    A class for interacting with the Gen3 submission services.
    Supports submitting and exporting from Sheepdog.
    Supports GraphQL queries through Peregrine.

    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Submission class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... sub = Gen3Submission(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint

    def query(self, query_txt, variables=None, max_tries=1):
        """Execute a GraphQL query against a data commons.

        Args:
            query_txt (str): Query text.
            variables (:obj:`object`, optional): Dictionary of variables to pass with the query.
            max_tries (:obj:`int`, optional): Number of times to retry if the request fails.
   
        Examples:
            This executes a query to get the list of all the project codes for all the projects
            in the data commons.

            >>> query = "{ project(first:0) { code } }"
            ... Gen3Submission.query(query)

        """
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

    def export_node(self, program, project, uuid):
        """Export a single node into json.

        Args:
            program (str): The program the node is under.
            project (str): The project the node is under.
            uuid (str): The UUID of the node to export.
   
        Examples:
            This exports a node from the sandbox commons.

            >>> Gen3Submission.export_node("DCF", "CCLE", "d70b41b9-6f90-4714-8420-e043ab8b77b9")

        """
        api_url = "{}/api/v0/submission/{}/{}/export?ids={}&format=json".format(
            self._endpoint, program, project, uuid
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        data = json.loads(output)
        return data

    def export_node_all_type(self, program, project, node_type):
        """Export all nodes of a single type.

        Args:
            program (str): The program to export nodes under.
            project (str): The project to export nodes under.
            node_type (str): The label of the node type to export.
   
        Examples:
            This exports all "sample" types from the CCLE project on the sandbox commons.

            >>> Gen3Submission.export_node_all_type("DCF", "CCLE", "sample")

        """
        api_url = "{}/api/v0/submission/{}/{}/export/?node_label={}&format=json".format(
            self._endpoint, program, project, node_type
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        data = json.loads(output)
        return data

    def submit_node(self, program, project, json):
        """Submit a node to a project.

        Args:
            program (str): The program to submit to.
            project (str): The project to submit to.
            json (object): The json of the node to submit
   
        Examples:
            This submits a node to the CCLE project on the sandbox commons.

            >>> Gen3Submission.submit_node("DCF", "CCLE", json)

        """
        api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
        output = requests.put(api_url, auth=self._auth_provider, json=json).text
        return output

    def delete_node(self, program, project, uuid):
        """Delete a node from a project.
        Args:
            program (str): The program to delete from.
            project (str): The project to delete from.
            uuid (str): The uuid of the node to delete
    
        Examples:
            This deletes a node from the CCLE project on the sandbox commons.
            
            >>> Gen3Submission.delete_node("DCF", "CCLE", uuid)
        """
        api_url = "{}/api/v0/submission/{}/{}/entities/{}".format(
            self._endpoint, program, project, uuid
        )
        output = requests.delete(api_url, auth=self._auth_provider).text
        return output

    def create_project(self, program, json):
        """Create a project.
        Args:
            program (str): The program to create a project on
            json (object): The json of the project to create
    
        Examples:
            This creates a project on the DCF program on the sandbox commons.
            
            >>> Gen3Submission.create_project("DCF", json)
        """
        api_url = "{}/api/v0/submission/{}".format(self._endpoint, program)
        output = requests.put(api_url, auth=self._auth_provider, json=json).text
        return output

    def delete_project(self, program, project):
        """Delete a project.

        This deletes an empty project from the commons.

        Args:
            program (str): The program containing the project to delete.
            project (str): The project to delete.
   
        Examples:
            This deletes the "CCLE" project from the "DCF" program.

            >>> Gen3Submission.delete_project("DCF", "CCLE")

        """
        api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
        output = requests.delete(api_url, auth=self._auth_provider).text
        return output

    def create_program(self, json):
        """Create a program.
        Args:
            json (object): The json of the program to create
    
        Examples:
            This creates a program on the sandbox commons.
            
            >>> Gen3Submission.create_program(json)
        """
        api_url = "{}/api/v0/submission/".format(self._endpoint)
        output = requests.post(api_url, auth=self._auth_provider, json=json).text
        return output

    def delete_program(self, program):
        """Delete a program.

        This deletes an empty program from the commons.

        Args:
            program (str): The program to delete.
   
        Examples:
            This deletes the "DCF" program.

            >>> Gen3Submission.delete_program("DCF")

        """
        api_url = "{}/api/v0/submission/{}".format(self._endpoint, program)
        output = requests.delete(api_url, auth=self._auth_provider).text
        return output
    
    def get_dictionary_node(self, node_type):
        """Returns the dictionary schema for a specific node.

        This gets the current json dictionary schema for a specific node type in a commons.

        Args:
            node_type (str): The node_type to retrieve.
   
        Examples:
            This returns the dictionary schema the "subject" node.

            >>> Gen3Submission.get_dictionary_node("subject")

        """
        api_url = "{}/api/v0/submission/_dictionary/{}".format(
            self._endpoint, node_type
        )
        output = requests.get(api_url).text
        data = json.loads(output)
        return data

    def get_dictionary_all(self):
        """Returns the dictionary object for a commons.

        This gets the current json dictionary schema for a commons.
   
        Examples:
            This returns the dictionary schema for a commons.

            >>> Gen3Submission.get_dictionary_all()

        """
        return self.get_dictionary_node("_all")

    def get_graphql_schema(self):
        """Returns the GraphQL schema for a commons.

        This runs the GraphQL introspection query against a commons and returns the results.
   
        Examples:
            This returns the GraphQL schema.

            >>> Gen3Submission.get_graphql_schema()

        """
        api_url = "{}/api/v0/submission/getschema".format(self._endpoint)
        output = requests.get(api_url).text
        data = json.loads(output)
        return data

import itertools
import json
import requests
import os
from cdislogging import get_logger
import pandas as pd

from gen3.utils import raise_for_status_and_print_error

logging = get_logger("__name__")


class Gen3Error(Exception):
    pass


class Gen3SubmissionQueryError(Gen3Error):
    pass


class Gen3UserError(Gen3Error):
    pass


class Gen3Submission:
    """Submit/Export/Query data from a Gen3 Submission system.

    A class for interacting with the Gen3 submission services.
    Supports submitting and exporting from Sheepdog.
    Supports GraphQL queries through Peregrine.

    Args:
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Submission class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... sub = Gen3Submission(auth)

    """

    def __init__(self, endpoint=None, auth_provider=None):
        # auth_provider legacy interface required endpoint as 1st arg
        self._auth_provider = auth_provider or endpoint
        self._endpoint = self._auth_provider.endpoint

    def __export_file(self, filename, output):
        """Writes an API response to a file."""
        with open(filename, "w") as outfile:
            outfile.write(output)
        print("\nOutput written to file: " + filename)

    ### Program functions

    def get_programs(self):
        """List registered programs"""
        api_url = f"{self._endpoint}/api/v0/submission/"
        output = requests.get(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(output)
        return output.json()

    def create_program(self, json):
        """Create a program.
        Args:
            json (object): The json of the program to create

        Examples:
            This creates a program in the sandbox commons.

            >>> Gen3Submission.create_program(json)
        """
        api_url = "{}/api/v0/submission/".format(self._endpoint)
        output = requests.post(api_url, auth=self._auth_provider, json=json)
        raise_for_status_and_print_error(output)
        return output.json()

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
        output = requests.delete(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(output)
        return output

    ### Project functions

    def get_projects(self, program):
        """List registered projects for a given program

        Args:
            program: the name of the program you want the projects from

        Example:
            This lists all the projects under the DCF program

            >>> Gen3Submission.get_projects("DCF")

        """
        api_url = f"{self._endpoint}/api/v0/submission/{program}"
        output = requests.get(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(output)
        return output.json()

    def create_project(self, program, json):
        """Create a project.
        Args:
            program (str): The program to create a project on
            json (object): The json of the project to create

        Examples:
            This creates a project on the DCF program in the sandbox commons.

            >>> Gen3Submission.create_project("DCF", json)
        """
        api_url = "{}/api/v0/submission/{}".format(self._endpoint, program)
        output = requests.put(api_url, auth=self._auth_provider, json=json)
        raise_for_status_and_print_error(output)
        return output.json()

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
        output = requests.delete(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(output)
        return output

    def get_project_dictionary(self, program, project):
        """Get dictionary schema for a given project

        Args:
            program: the name of the program the project is from
            project: the name of the project you want the dictionary schema from

        Example:

            >>> Gen3Submission.get_project_dictionary("DCF", "CCLE")

        """
        api_url = f"{self._endpoint}/api/v0/submission/{program}/{project}/_dictionary"
        output = requests.get(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(output)
        return output.json()

    def open_project(self, program, project):
        """Mark a project ``open``. Opening a project means uploads, deletions, etc. are allowed.

        Args:
            program: the name of the program the project is from
            project: the name of the project you want to 'open'

        Example:

            >>> Gen3Submission.get_project_manifest("DCF", "CCLE")

        """
        api_url = f"{self._endpoint}/api/v0/submission/{program}/{project}/open"
        output = requests.put(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(output)
        return output.json()

    ### Record functions

    def submit_record(self, program, project, json):
        """Submit record(s) to a project as json.

        Args:
            program (str): The program to submit to.
            project (str): The project to submit to.
            json (object): The json defining the record(s) to submit. For multiple records, the json should be an array of records.

        Examples:
            This submits records to the CCLE project in the sandbox commons.

            >>> Gen3Submission.submit_record("DCF", "CCLE", json)

        """
        api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
        logging.info("\nUsing the Sheepdog API URL {}\n".format(api_url))

        output = requests.put(api_url, auth=self._auth_provider, json=json)
        output.raise_for_status()
        return output.json()

    def delete_record(self, program, project, uuid):
        """
        Delete a record from a project.

        Args:
            program (str): The program to delete from.
            project (str): The project to delete from.
            uuid (str): The uuid of the record to delete

        Examples:
            This deletes a record from the CCLE project in the sandbox commons.

            >>> Gen3Submission.delete_record("DCF", "CCLE", uuid)
        """
        return self.delete_records(program, project, [uuid])

    def delete_records(self, program, project, uuids, batch_size=100):
        """
        Delete a list of records from a project.

        Args:
            program (str): The program to delete from.
            project (str): The project to delete from.
            uuids (list): The list of uuids of the records to delete
            batch_size (int, optional, default: 100): how many records to delete at a time

        Examples:
            This deletes a list of records from the CCLE project in the sandbox commons.

            >>> Gen3Submission.delete_records("DCF", "CCLE", ["uuid1", "uuid2"])
        """
        api_url = "{}/api/v0/submission/{}/{}/entities".format(
            self._endpoint, program, project
        )
        for i in itertools.count():
            uuids_to_delete = uuids[batch_size * i : batch_size * (i + 1)]
            if len(uuids_to_delete) == 0:
                break
            output = requests.delete(
                "{}/{}".format(api_url, ",".join(uuids_to_delete)),
                auth=self._auth_provider,
            )
            try:
                raise_for_status_and_print_error(output)
            except requests.exceptions.HTTPError:
                print(
                    "\n{}\nFailed to delete uuids: {}".format(
                        output.text, uuids_to_delete
                    )
                )
                raise
        return output

    def delete_node(self, program, project, node_name, batch_size=100, verbose=True):
        """
        Delete all records for a node from a project.

        Args:
            program (str): The program to delete from.
            project (str): The project to delete from.
            node_name (str): Name of the node to delete
            batch_size (int, optional, default: 100): how many records to query and delete at a time
            verbose (bool, optional, default: True): whether to print progress logs

        Examples:
            This deletes a node from the CCLE project in the sandbox commons.

            >>> Gen3Submission.delete_node("DCF", "CCLE", "demographic")
        """
        return self.delete_nodes(
            program, project, [node_name], batch_size, verbose=verbose
        )

    def delete_nodes(
        self, program, project, ordered_node_list, batch_size=100, verbose=True
    ):
        """
        Delete all records for a list of nodes from a project.

        Args:
            program (str): The program to delete from.
            project (str): The project to delete from.
            ordered_node_list (list): The list of nodes to delete, in reverse graph submission order
            batch_size (int, optional, default: 100): how many records to query and delete at a time
            verbose (bool, optional, default: True): whether to print progress logs

        Examples:
            This deletes a list of nodes from the CCLE project in the sandbox commons.

            >>> Gen3Submission.delete_nodes("DCF", "CCLE", ["demographic", "subject", "experiment"])
        """
        project_id = f"{program}-{project}"
        for node in ordered_node_list:
            if verbose:
                print(node, end="", flush=True)
            first_uuid = ""
            while True:
                query_string = f"""{{
                    {node} (first: {batch_size}, project_id: "{project_id}") {{
                        id
                    }}
                }}"""
                res = self.query(query_string)
                uuids = [x["id"] for x in res["data"][node]]
                if len(uuids) == 0:
                    break  # all done
                if first_uuid == uuids[0]:
                    raise Exception("Failed to delete. Exiting")
                first_uuid = uuids[0]
                if verbose:
                    print(".", end="", flush=True)
                self.delete_records(program, project, uuids, batch_size)
            if verbose:
                print()

    def export_record(self, program, project, uuid, fileformat, filename=None):
        """Export a single record into json.

        Args:
            program (str): The program the record is under.
            project (str): The project the record is under.
            uuid (str): The UUID of the record to export.
            fileformat (str): Export data as either 'json' or 'tsv'
            filename (str): Name of the file to export to; if no filename is provided, prints data to screen

        Examples:
            This exports a single record from the sandbox commons.

            >>> Gen3Submission.export_record("DCF", "CCLE", "d70b41b9-6f90-4714-8420-e043ab8b77b9", "json", filename="DCF-CCLE_one_record.json")

        """
        assert fileformat in [
            "json",
            "tsv",
        ], "File format must be either 'json' or 'tsv'"
        api_url = "{}/api/v0/submission/{}/{}/export?ids={}&format={}".format(
            self._endpoint, program, project, uuid, fileformat
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        if filename is None:
            if fileformat == "json":
                try:
                    output = json.loads(output)
                except ValueError as e:
                    print(f"Output: {output}\nUnable to parse JSON: {e}")
                    raise
            return output
        else:
            self.__export_file(filename, output)
            return output

    def export_node(self, program, project, node_type, fileformat, filename=None):
        """Export all records in a single node type of a project.

        Args:
            program (str): The program to which records belong.
            project (str): The project to which records belong.
            node_type (str): The name of the node to export.
            fileformat (str): Export data as either 'json' or 'tsv'
            filename (str): Name of the file to export to; if no filename is provided, prints data to screen

        Examples:
            This exports all records in the "sample" node from the CCLE project in the sandbox commons.

            >>> Gen3Submission.export_node("DCF", "CCLE", "sample", "tsv", filename="DCF-CCLE_sample_node.tsv")

        """
        assert fileformat in [
            "json",
            "tsv",
        ], "File format must be either 'json' or 'tsv'"
        api_url = "{}/api/v0/submission/{}/{}/export/?node_label={}&format={}".format(
            self._endpoint, program, project, node_type, fileformat
        )
        output = requests.get(api_url, auth=self._auth_provider).text
        if filename is None:
            if fileformat == "json":
                try:
                    output = json.loads(output)
                except ValueError as e:
                    print(f"Output: {output}\nUnable to parse JSON: {e}")
                    raise
            return output
        else:
            self.__export_file(filename, output)
            return output

    ### Query functions

    def query(self, query_txt, variables=None, max_tries=1):
        """Execute a GraphQL query against a Data Commons.

        Args:
            query_txt (str): Query text.
            variables (:obj:`object`, optional): Dictionary of variables to pass with the query.
            max_tries (:obj:`int`, optional): Number of times to retry if the request fails.

        Examples:
            This executes a query to get the list of all the project codes for all the projects
            in the Data Commons.

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

    ### Dictionary functions

    def get_dictionary_node(self, node_type):
        """Returns the dictionary schema for a specific node.

        This gets the current json dictionary schema for a specific node type in a commons.

        Args:
            node_type (str): The node_type (or name of the node) to retrieve.

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
        """Returns the entire dictionary object for a commons.

        This gets a json of the current dictionary schema for a commons.

        Examples:
            This returns the dictionary schema for a commons.

            >>> Gen3Submission.get_dictionary_all()

        """
        return self.get_dictionary_node("_all")

    ### File functions

    def get_project_manifest(self, program, project):
        """Get a projects file manifest

        Args:
            program: the name of the program the project is from
            project: the name of the project you want the manifest from

        Example:

            >>> Gen3Submission.get_project_manifest("DCF", "CCLE")

        """
        api_url = f"{self._endpoint}/api/v0/submission/{program}/{project}/manifest"
        output = requests.get(api_url, auth=self._auth_provider)
        return output

    def submit_file(self, project_id, filename, chunk_size=30, row_offset=0):
        """Submit data in a spreadsheet file containing multiple records in rows to a Gen3 Data Commons.

        Args:
            project_id (str): The project_id to submit to.
            filename (str): The file containing data to submit. The format can be TSV, CSV or XLSX (first worksheet only for now).
            chunk_size (integer): The number of rows of data to submit for each request to the API.
            row_offset (integer): The number of rows of data to skip; '0' starts submission from the first row and submits all data.

        Examples:
            This submits a spreadsheet file containing multiple records in rows to the CCLE project in the sandbox commons.

            >>> Gen3Submission.submit_file("DCF-CCLE","data_spreadsheet.tsv")

        """
        # Read the file in as a pandas DataFrame
        f = os.path.basename(filename)
        if f.lower().endswith(".csv"):
            df = pd.read_csv(filename, header=0, sep=",", dtype=str).fillna("")
        elif f.lower().endswith(".xlsx"):
            xl = pd.ExcelFile(filename)  # load excel file
            sheet = xl.sheet_names[0]  # sheetname
            df = xl.parse(sheet)  # save sheet as dataframe
            converters = {
                col: str for col in list(df)
            }  # make sure int isn't converted to float
            df = pd.read_excel(filename, converters=converters).fillna("")  # remove nan
        elif filename.lower().endswith((".tsv", ".txt")):
            df = pd.read_csv(filename, header=0, sep="\t", dtype=str).fillna("")
        else:
            raise Gen3UserError("Please upload a file in CSV, TSV, or XLSX format.")
        df.rename(
            columns={c: c.lstrip("*") for c in df.columns}, inplace=True
        )  # remove any leading asterisks in the DataFrame column names

        # Check uniqueness of submitter_ids:
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            raise Gen3Error(
                "Warning: file contains duplicate submitter_ids. \nNote: submitter_ids must be unique within a node!"
            )

        # Chunk the file
        print("\nSubmitting {} with {} records.".format(filename, len(df)))
        program, project = project_id.split("-", 1)
        api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
        headers = {"content-type": "text/tab-separated-values"}

        start = row_offset
        end = row_offset + chunk_size
        chunk = df[start:end]

        count = 0

        results = {
            "invalid": {},  # these are invalid records
            "other": [],  # any unhandled API responses
            "details": [],  # entire API response details
            "succeeded": [],  # list of submitter_ids that were successfully updated/created
            "responses": [],  # list of API response codes
        }

        # Start the chunking loop:
        while (start + len(chunk)) <= len(df):
            timeout = False
            valid_but_failed = []
            invalid = []
            count += 1
            print(
                "Chunk {} (chunk size: {}, submitted: {} of {})".format(
                    count,
                    chunk_size,
                    len(results["succeeded"]) + len(results["invalid"]),
                    len(df),
                )
            )

            try:
                response = requests.put(
                    api_url,
                    auth=self._auth_provider,
                    data=chunk.to_csv(sep="\t", index=False),
                    headers=headers,
                ).text
            except requests.exceptions.ConnectionError as e:
                results["details"].append(e.message)
                continue

            # Handle the API response
            if (
                "Request Timeout" in response
                or "413 Request Entity Too Large" in response
                or "Connection aborted." in response
                or "service failure - try again later" in response
            ):  # time-out, response is not valid JSON at the moment
                print("\t Reducing Chunk Size: {}".format(response))
                results["responses"].append("Reducing Chunk Size: {}".format(response))
                timeout = True

            else:
                try:
                    json_res = json.loads(response)
                except ValueError as e:
                    print(response)
                    print(str(e))
                    raise Gen3Error("Unable to parse API response as JSON!")

                if "message" in json_res and "code" not in json_res:
                    print(
                        "\t No code in the API response for Chunk {}: {}".format(
                            count, json_res.get("message")
                        )
                    )
                    print("\t {}".format(json_res.get("transactional_errors")))
                    results["responses"].append(
                        "Error Chunk {}: {}".format(count, json_res.get("message"))
                    )
                    results["other"].append(json_res.get("transactional_errors"))

                elif "code" not in json_res:
                    print("\t Unhandled API-response: {}".format(response))
                    results["responses"].append(
                        "Unhandled API response: {}".format(response)
                    )

                elif json_res["code"] == 200:  # success
                    entities = json_res.get("entities", [])
                    print("\t Succeeded: {} entities.".format(len(entities)))
                    results["responses"].append(
                        "Chunk {} Succeeded: {} entities.".format(count, len(entities))
                    )

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        results["succeeded"].append(sid)

                elif json_res["code"] == 500:  # internal server error
                    print("\t Internal Server Error: {}".format(response))
                    results["responses"].append(
                        "Internal Server Error: {}".format(response)
                    )

                else:  # failure (400, 401, 403, 404...)
                    entities = json_res.get("entities", [])
                    print(
                        "\tChunk Failed (status code {}): {} entities.".format(
                            json_res.get("code"), len(entities)
                        )
                    )
                    results["responses"].append(
                        "Chunk {} Failed: {} entities.".format(count, len(entities))
                    )

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        if entity["valid"]:  # valid but failed
                            valid_but_failed.append(sid)
                        else:  # invalid and failed
                            message = str(entity["errors"])
                            results["invalid"][sid] = message
                            invalid.append(sid)
                    print("\tInvalid records in this chunk: {}".format(len(invalid)))

            if (
                len(valid_but_failed) > 0 and len(invalid) > 0
            ):  # if valid entities failed bc grouped with invalid, retry submission
                chunk = chunk.loc[
                    df["submitter_id"].isin(valid_but_failed)
                ]  # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
                print(
                    "Retrying submission of valid entities from failed chunk: {} valid entities.".format(
                        len(chunk)
                    )
                )

            elif (
                len(valid_but_failed) > 0 and len(invalid) == 0
            ):  # if all entities are valid but submission still failed, probably due to duplicate submitter_ids. Can remove this section once the API response is fixed: https://ctds-planx.atlassian.net/browse/PXP-3065
                raise Gen3Error(
                    "Please check your data for correct file encoding, special characters, or duplicate submitter_ids or ids."
                )

            elif timeout is False:  # get new chunk if didn't timeout
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            else:  # if timeout, reduce chunk size and retry smaller chunk
                if chunk_size >= 2:
                    chunk_size = int(chunk_size / 2)
                    end = start + chunk_size
                    chunk = df[start:end]
                    print(
                        "Retrying Chunk with reduced chunk_size: {}".format(chunk_size)
                    )
                    timeout = False
                else:
                    print("Last chunk:\n{}".format(chunk))
                    raise Gen3Error(
                        "Submission is timing out. Please contact the Helpdesk."
                    )

        print("Finished data submission.")
        print("Successful records: {}".format(len(set(results["succeeded"]))))
        print("Failed invalid records: {}".format(len(results["invalid"])))

        return results

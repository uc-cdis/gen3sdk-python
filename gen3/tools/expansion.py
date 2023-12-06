import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re, operator, csv
from os import path
import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

from pandas import json_normalize
import pandas as pd


class Gen3Error(Exception):
    pass


class Gen3Expansion:
    """Advanced scripts for interacting with the Gen3 submission, query and index APIs
    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Expansion class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... exp = Gen3Expansion(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint
        self.sub = Gen3Submission(endpoint, auth_provider)

    # Functions for downloading metadata in TSVs

    def get_project_ids(self, node=None, name=None):
        """Get a list of project_ids you have access to in a data commons.

        Args:
            node(str): The node you want projects to have at least one record in.
            name(str): The name of the programs to get projects in, or the submitter_id of a particular record.

        Example:
            get_project_ids()
            get_project_ids(node='demographic')
            get_project_ids(node='program',name='training')
            get_project_ids(node='case',name='case-01')
        """
        project_ids = []
        queries = []
        # Return all project_ids in the data commons if no node is provided or if node is program but no name provided
        if name is None and ((node is None) or (node is "program")):
            print("Getting all project_ids you have access to in the data commons.")
            if node == "program":
                print(
                    """Specify a list of program names (name = ['myprogram1','myprogram2']) to get only project_ids in particular programs."""
                )
            queries.append("""{project (first:0){project_id}}""")
        elif name is not None and node == "program":
            if isinstance(name, list):
                print(
                    """"Getting all project_ids in the programs '%s'"""
                    % (",".join(name))
                )
                for program_name in name:
                    queries.append(
                        """{project (first:0, with_path_to:{type:"program",name:"%s"}){project_id}}"""
                        % (program_name)
                    )
            elif isinstance(name, str):
                print("Getting all project_ids in the program '" + name + "'")
                queries.append(
                    """{project (first:0, with_path_to:{type:"program",name:"%s"}){project_id}}"""
                    % (name)
                )
        elif isinstance(node, str) and isinstance(name, str):
            print(
                """Getting all project_ids for projects with a path to record '%s' in node '%s'"""
                % (node, name)
            )
            queries.append(
                """{project (first:0, with_path_to:{type:"%s",submitter_id:"%s"}){project_id}}"""
                % (node, name)
            )
        elif isinstance(node, str) and name is None:
            print(
                """Getting all project_ids for projects with at least one record in the node '%s'"""
                % (node)
            )
            query = """{node (first:0,of_type:"%s"){project_id}}""" % (node)
            # Need separate logic because we are looking at res["data"]["node"]
            res = self.sub.query(query)
            new_project_ids = list(set([x["project_id"] for x in res["data"]["node"]]))
            project_ids = project_ids + new_project_ids

        if len(queries) > 0:
            for query in queries:
                res = self.sub.query(query)
                new_project_ids = list(
                    set([x["project_id"] for x in res["data"]["project"]])
                )
                project_ids = project_ids + new_project_ids
        my_ids = sorted(project_ids, key=str.lower)
        print(my_ids)
        return my_ids

    def get_node_tsvs(
        self,
        node,
        projects=None,
        overwrite=False,
        remove_empty=True,
        outdir="node_tsvs",
    ):
        """Gets a TSV of the structured data from particular node for each project specified.
           Also creates a master TSV of merged data from each project for the specified node.
           Returns a DataFrame containing the merged data for the specified node.

        Args:
            node (str): The name of the node to download structured data from.
            projects (list): The projects to download the node from. If "None", downloads data from each project user has access to.

        Example:
        >>> df = get_node_tsvs('demographic')

        """

        if not os.path.exists(outdir):
            os.makedirs(outdir)
        mydir = "{}/{}_tsvs".format(outdir, node)
        if not os.path.exists(mydir):
            os.makedirs(mydir)

        if projects is None:  # if no projects specified, get node for all projects
            projects = sorted(
                list(
                    set(
                        x["project_id"]
                        for x in self.sub.query("""{project (first:0){project_id}}""")[
                            "data"
                        ]["project"]
                    )
                )
            )
        elif isinstance(projects, str):
            projects = [projects]

        header = ""
        output = ""
        data_count = 0

        for project in projects:
            filename = str(mydir + "/" + project + "_" + node + ".tsv")
            if (os.path.isfile(filename)) and (overwrite is False):
                print("File previously downloaded.")
            else:
                prog, proj = project.split("-", 1)
                self.sub.export_node(prog, proj, node, "tsv", filename)

            with open(filename) as tsv_file:
                count = 0
                lines = tsv_file.readlines()
                # if file contains actual data and not just header row
                if len(lines) > 1:
                    for line in lines:
                        count += 1
                        # If header is empty and we are at the first line, set the header
                        if len(header) == 0 and count == 1:
                            header = line
                        elif count > 1:
                            output = output + line
                    count -= 1
                    data_count += count
                print(filename + " has " + str(count) + " records.")

                if remove_empty is True:
                    if count == 0:
                        print("Removing empty file: " + filename)
                        cmd = ["rm", filename]  # look in the download directory
                        try:
                            output = subprocess.check_output(
                                cmd, stderr=subprocess.STDOUT
                            ).decode("UTF-8")
                        except Exception as e:
                            output = e.output.decode("UTF-8")
                            print("ERROR deleting file: " + output)
        print("length of all data: " + str(data_count))
        nodefile = str("master_" + node + ".tsv")
        # Append header to the beginning of TSV
        output = header + output
        with open(nodefile, "w+") as master_tsv:
            master_tsv.write(output)
        print(
            "Master node TSV with "
            + str(data_count)
            + " total records written to "
            + nodefile
            + "."
        )
        return output

    def get_project_tsvs(
        self,
        projects=None,
        nodes=None,
        outdir="project_tsvs",
        overwrite=False,
        save_empty=False,
        remove_nodes=["program", "project", "root", "data_release"],
    ):
        """Function gets a TSV for every node in a specified project.
            Exports TSV files into a directory "project_tsvs/".
            Function returns a list of the contents of the directory.
        Args:
            projects (str/list): The project_id(s) of the project(s) to download. Can be a single project_id or a list of project_ids.
            nodes(str/list): The nodes to download from each project. If None, will try to download all nodes in the data model.
            overwrite (boolean): If False, the TSV file is not downloaded if there is an existing file with the same name.
            save_empty(boolean): If True, TSVs with no records, i.e., downloads an empty TSV template, will be downloaded.
            remove_nodes(list): A list of nodes in the data model that should not be downloaded per project.
        Example:
        >>> get_project_tsvs(projects = ['internal-test'])

        """
        if nodes is None:
            # get all the 'node_id's in the data model
            nodes = sorted(
                list(
                    set(
                        x["id"]
                        for x in self.sub.query("""{_node_type (first:-1) {id}}""")[
                            "data"
                        ]["_node_type"]
                    )
                )
            )
        elif isinstance(nodes, str):
            nodes = [nodes]

        for node in remove_nodes:
            if node in nodes:
                nodes.remove(node)

        if projects is None:  # if no projects specified, get node for all projects
            projects = sorted(
                list(
                    set(
                        x["project_id"]
                        for x in self.sub.query("""{project (first:0){project_id}}""")[
                            "data"
                        ]["project"]
                    )
                )
            )
        elif isinstance(projects, str):
            projects = [projects]

        for project_id in projects:
            mydir = "{}/{}_tsvs".format(
                outdir, project_id
            )  # create the directory to store TSVs

            if not os.path.exists(mydir):
                os.makedirs(mydir)

            for node in nodes:
                filename = str(mydir + "/" + project_id + "_" + node + ".tsv")
                if (os.path.isfile(filename)) and (overwrite is False):
                    print("\tPreviously downloaded: '{}'".format(filename))
                else:
                    query_txt = """{_%s_count (project_id:"%s")}""" % (node, project_id)
                    res = self.sub.query(
                        query_txt
                    )  #  {'data': {'_acknowledgement_count': 0}}
                    count = res["data"][str("_" + node + "_count")]  # count=int(0)
                    if count > 0 or save_empty is True:
                        print(
                            "\nDownloading {} records in node '{}' of project '{}'.".format(
                                count, node, project_id
                            )
                        )
                        prog, proj = project_id.split("-", 1)
                        self.sub.export_node(prog, proj, node, "tsv", filename)
                    else:
                        print(
                            "\t{} records in node '{}' of project '{}'.".format(
                                count, node, project_id
                            )
                        )

        cmd = ["ls", mydir]  # look in the download directory
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(
                "UTF-8"
            )
        except Exception as e:
            output = "ERROR:" + e.output.decode("UTF-8")

        return output

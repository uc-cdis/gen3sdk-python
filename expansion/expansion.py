import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re, operator, statistics, datetime, hashlib, uuid
import pandas as pd
from os import path
from pandas import json_normalize
from collections import Counter
from statistics import mean
from io import StringIO
from IPython.utils import io
from itertools import cycle
import random
from random import randrange
from pathlib import Path

import numpy as np
import scipy
import matplotlib.pyplot as plt
import seaborn as sns


class Gen3Error(Exception):
    pass


class Gen3Expansion:
    """Advanced scripts for interacting with the Gen3 submission, query and index APIs

    Supports advanced data submission and exporting from Sheepdog.
    Supports paginated GraphQL queries through Peregrine.
    Supports Flat Model (ElasticSearch) queries through Arranger/Guppy.
    Supports Indexd queries.
    Supports user authentication queries.

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

    def __init__(self, endpoint, auth_provider, submission):
        self._auth_provider = auth_provider
        self._endpoint = endpoint
        self.sub = submission  # submission is Gen3Submission(endpoint, auth_provider)

    def __export_file(self, filename, output):
        """Writes text, e.g., an API response, to a file.
        Args:
            filename (str): The name of the file to be created.
            output (str): The contents of the file to be created.
        Example:
        >>> output = requests.get(api_url, auth=self._auth_provider).text
        ... self.__export_file(filename, output)
        """
        outfile = open(filename, "w")
        outfile.write(output)
        outfile.close
        print("Output written to file: " + filename + "\n")

    ### AWS S3 Tools:
    def s3_ls(self, path, bucket, profile, pattern="*"):
        """ Print the results of an `aws s3 ls` command """
        s3_path = bucket + path
        cmd = ["aws", "s3", "ls", s3_path, "--profile", profile]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(
                "UTF-8"
            )
        except Exception as e:
            output = e.output.decode("UTF-8")
            print("ERROR:" + output)
        psearch = output.split("\n")
        if pattern != "*":
            pmatch = fnmatch.filter(
                psearch, pattern
            )  # if default '*', all files will match
            return arrayTable(pmatch)
        else:
            return output

    def s3_files(self, path, bucket, profile, pattern="*", verbose=True):
        """ Get a list of files returned by an `aws s3 ls` command """
        s3_path = bucket + path
        cmd = ["aws", "s3", "ls", s3_path, "--profile", profile]
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, shell=True
            ).decode("UTF-8")
        except Exception as e:
            output = e.output.decode("UTF-8")
            print("ERROR:" + output)
        output = [line.split() for line in output.split("\n")]
        output = [
            line for line in output if len(line) == 4
        ]  # filter output for lines with file info
        output = [line[3] for line in output]  # grab the filename only
        output = fnmatch.filter(output, pattern)  # if default '*', all files will match
        if verbose == True:
            print("\nIndex \t Filename")
            for (i, item) in enumerate(output, start=0):
                print(i, "\t", item)
        return output

    def get_s3_files(self, path, bucket, profile, files=None, mydir=None):
        """ Transfer data from object storage to the VM in the private subnet """

        # Set the path to the directory where files reside
        s3_path = bucket + path

        # Create folder on VM for downloaded files
        if not isinstance(mydir, str):
            mydir = path
        if not os.path.exists(mydir):
            os.makedirs(mydir)

        # If files is an array of filenames, download them
        if isinstance(files, list):
            print("Getting files...")
            for filename in files:
                s3_filepath = s3_path + str(filename)
                if os.path.exists(mydir + str(filename)):
                    print("File " + filename + " already downloaded in that location.")
                else:
                    print(s3_filepath)
                    cmd = ["aws", "s3", "--profile", profile, "cp", s3_filepath, mydir]
                    try:
                        output = subprocess.check_output(
                            cmd, stderr=subprocess.STDOUT, shell=True
                        ).decode("UTF-8")
                    except Exception as e:
                        output = e.output.decode("UTF-8")
                        print("ERROR:" + output)
        # If files == None, which syncs the s3_path 'directory'
        else:
            print("Syncing directory " + s3_path)
            cmd = ["aws", "s3", "--profile", profile, "sync", s3_path, mydir]
            try:
                output = subprocess.check_output(
                    cmd, stderr=subprocess.STDOUT, shell=True
                ).decode("UTF-8")
            except Exception as e:
                output = e.output.decode("UTF-8")
                print("ERROR:" + output)
        print("Finished")

    # Functions for downloading metadata in TSVs

    def get_project_ids(self, node=None, name=None):
        """Get a list of project_ids you have access to in a data commons.

        Args:
            node(str): The node you want projects to have at least one record in.
            name(str): The name of the programs to get projects in, or the submitter_id of a particular record.

        Example:
            get_project_ids()
            get_project_ids(node='demographic')
            get_project_ids(node='program',name=['training','internal'])
            get_project_ids(node='case',name='case-01')
        """
        project_ids = []
        queries = []
        # Return all project_ids in the data commons if no node is provided or if node is program but no name provided
        if name == None and ((node == None) or (node == "program")):
            print("Getting all project_ids you have access to in the data commons.")
            if node == "program":
                print(
                    "Specify a list of program names (name = ['myprogram1','myprogram2']) to get only project_ids in particular programs."
                )
            queries.append("""{project (first:0){project_id}}""")
        elif name != None and node == "program":
            if isinstance(name, list):
                print(
                    "Getting all project_ids in the programs '" + ",".join(name) + "'"
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
                "Getting all project_ids for projects with a path to record '"
                + name
                + "' in node '"
                + node
                + "'"
            )
            queries.append(
                """{project (first:0, with_path_to:{type:"%s",submitter_id:"%s"}){project_id}}"""
                % (node, name)
            )
        elif isinstance(node, str) and name == None:
            print(
                "Getting all project_ids for projects with at least one record in the node '"
                + node
                + "'"
            )
            query = """{node (first:0,of_type:"%s"){project_id}}""" % (node)
            df = pd.json_normalize(self.sub.query(query)["data"]["node"])
            project_ids = project_ids + list(set(df["project_id"]))
        if len(queries) > 0:
            for query in queries:
                res = self.sub.query(query)
                df = pd.json_normalize(res["data"]["project"])
                project_ids = project_ids + list(set(df["project_id"]))
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
        """Gets a TSV of the structuerd data from particular node for each project specified.
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

        if projects == None:  # if no projects specified, get node for all projects
            projects = list(
                json_normalize(
                    self.sub.query("""{project (first:0){project_id}}""")["data"][
                        "project"
                    ]
                )["project_id"]
            )
        elif isinstance(projects, str):
            projects = [projects]

        dfs = []
        df_len = 0
        for project in projects:
            filename = str(mydir + "/" + project + "_" + node + ".tsv")
            if (os.path.isfile(filename)) and (overwrite == False):
                print("File previously downloaded.")
            else:
                prog, proj = project.split("-", 1)
                self.sub.export_node(prog, proj, node, "tsv", filename)
            df1 = pd.read_csv(filename, sep="\t", header=0, index_col=False)
            df_len += len(df1)
            if not df1.empty:
                dfs.append(df1)

            print(filename + " has " + str(len(df1)) + " records.")

            if remove_empty == True:
                if df1.empty:
                    print("Removing empty file: " + filename)
                    cmd = ["rm", filename]  # look in the download directory
                    try:
                        output = subprocess.check_output(
                            cmd, stderr=subprocess.STDOUT
                        ).decode("UTF-8")
                    except Exception as e:
                        output = e.output.decode("UTF-8")
                        print("ERROR deleting file: " + output)

        all_data = pd.concat(dfs, ignore_index=True, sort=False)
        print("length of all dfs: " + str(df_len))
        nodefile = str("master_" + node + ".tsv")
        all_data.to_csv(str(mydir + "/" + nodefile), sep="\t", index=False)
        print(
            "Master node TSV with "
            + str(len(all_data))
            + " total records written to "
            + nodefile
            + "."
        )
        return all_data

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
            overwrite (boolean): If False, the TSV file != downloaded if there is an existing file with the same name.
            save_empty(boolean): If True, TSVs with no records, i.e., downloads an empty TSV template, will be downloaded.
            remove_nodes(list): A list of nodes in the data model that should not be downloaded per project.
        Example:
        >>> get_project_tsvs(projects = ['internal-test'])

        """
        if nodes == None:
            nodes = sorted(
                list(
                    set(
                        pd.json_normalize(
                            self.sub.query("""{_node_type (first:-1) {id}}""")["data"][
                                "_node_type"
                            ]
                        )["id"]
                    )
                )
            )  # get all the 'node_id's in the data model
        elif isinstance(nodes, str):
            nodes = [nodes]

        for node in remove_nodes:
            if node in nodes:
                nodes.remove(node)

        if projects == None:  # if no projects specified, get node for all projects
            projects = list(
                pd.json_normalize(
                    self.sub.query("""{project (first:0){project_id}}""")["data"][
                        "project"
                    ]
                )["project_id"]
            )
        elif isinstance(projects, str):
            projects = [projects]

        # now = datetime.datetime.now()
        # date = "{}-{}-{}-{}.{}.{}".format(now.year, now.month, now.day, now.hour, now.minute, now.second)

        for project_id in projects:
            #mydir = "{}_{}/{}_tsvs".format(outdir, date, project_id)  # create the directory to store TSVs
            mydir = "{}/{}_tsvs".format(outdir, project_id)  # create the directory to store TSVs

            if not os.path.exists(mydir):
                os.makedirs(mydir)

            for node in nodes:
                filename = str(mydir + "/" + project_id + "_" + node + ".tsv")
                if (os.path.isfile(filename)) and (overwrite == False):
                    print("\tPreviously downloaded: '{}'".format(filename))
                else:
                    query_txt = """{_%s_count (project_id:"%s")}""" % (node, project_id)
                    res = self.sub.query(
                        query_txt
                    )  #  {'data': {'_acknowledgement_count': 0}}
                    count = res["data"][str("_" + node + "_count")]  # count=int(0)
                    if count > 0 or save_empty == True:
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

    # Query Functions
    def query_counts(
        self,
        nodes,
        project_id=None,
        chunk_size=2500,
        format="json",
        args=None,
    ):
        """Function to paginate a query to avoid time-outs.
        Returns a json of all the records in the node.

        Args:
            nodes (list): The nodes to get counts for.
            project_id(str): The project_id to limit the query to. Default == None.
            chunk_size(int): The number of records to return per query. Default is 10000.
            args(str): Put graphQL arguments here. For example, 'with_path_to:{type:"case",submitter_id:"case-01"}', etc. Don't enclose in parentheses.
        Example:
            exp.query_counts(project_id='Exhale-Tempus',nodes='case')
        """

        counts = {}

        if isinstance(nodes,str):
            nodes = [nodes]

        for node in nodes:
            if project_id != None:
                program, project = project_id.split("-", 1)
                if args == None:
                    query_txt = """{_%s_count (project_id:"%s")}""" % (node, project_id)
                else:
                    query_txt = """{_%s_count (project_id:"%s", %s)}""" % (node, project_id, args)
            else:
                if args == None:
                    query_txt = """{_%s_count}""" % (node)
                else:
                    query_txt = """{_%s_count (%s)}""" % (node, args)

            # First query the node count to get the expected number of results for the requested query:

            try:
                res = self.sub.query(query_txt)
                count_name = "_".join(map(str, ["", node, "count"]))
                qsize = res["data"][count_name]
                counts[node] = qsize
            except:
                print("\n\tQuery to get _{}_count failed! {}".format(node, query_txt))


        return counts


    # Query Functions
    def paginate_query(
        self,
        node,
        project_id=None,
        props=["id", "submitter_id"],
        chunk_size=2500,
        format="json",
        args=None,
    ):
        """Function to paginate a query to avoid time-outs.
        Returns a json of all the records in the node.

        Args:
            node (str): The node to query.
            project_id (str): The project_id to limit the query to. Default == None.
            props (list): A list of properties in the node to return.
            chunk_size (int): The number of records to return per query. Default is 2500.
            args (str): Put graphQL arguments here. For example, 'with_path_to:{type:"case",submitter_id:"case-01"}', etc. Don't enclose in parentheses.
        Example:
            paginate_query('demographic',format='tsv')
            paginate_query('',args='with_path_to:{type:"case",submitter_id:"case-01"}')
        """

        if node == "datanode":
            query_txt = """{ %s (%s) { type } }""" % (node, args)
            response = self.sub.query(query_txt)
            if "data" in response:
                nodes = [record["type"] for record in response["data"]["datanode"]]
                if len(nodes) > 1:
                    print(
                        "\tMultiple files with that file_name exist across multiple nodes:\n\t{}.".format(
                            nodes
                        )
                    )
                elif len(nodes) == 1:
                    node = nodes[0]
                else:
                    return nodes

        if project_id != None:
            program, project = project_id.split("-", 1)
            if args == None:
                query_txt = """{_%s_count (project_id:"%s")}""" % (node, project_id)
            else:
                query_txt = """{_%s_count (project_id:"%s", %s)}""" % (
                    node,
                    project_id,
                    args,
                )
        else:
            if args == None:
                query_txt = """{_%s_count}""" % (node)
            else:
                query_txt = """{_%s_count (%s)}""" % (node, args)

        # First query the node count to get the expected number of results for the requested query:

        try:
            res = self.sub.query(query_txt)
            count_name = "_".join(map(str, ["", node, "count"]))
            qsize = res["data"][count_name]
            print(
                "\n\tFound {} records in '{}' node of project '{}'. ".format(
                    qsize, node, project_id
                )
            )
        except:
            print("\n\tQuery to get _{}_count failed! {}".format(node, query_txt))

        # Now paginate the actual query:
        properties = " ".join(map(str, props))
        offset = 0
        total = {}
        total["data"] = {}
        total["data"][node] = []
        count = 0
        while offset < qsize:

            if project_id != None:
                if args == None:
                    query_txt = (
                        """{%s (first: %s, offset: %s, project_id:"%s"){%s}}"""
                        % (node, chunk_size, offset, project_id, properties)
                    )
                else:
                    query_txt = (
                        """{%s (first: %s, offset: %s, project_id:"%s", %s){%s}}"""
                        % (node, chunk_size, offset, project_id, args, properties)
                    )
            else:
                if args == None:
                    query_txt = """{%s (first: %s, offset: %s){%s}}""" % (
                        node,
                        chunk_size,
                        offset,
                        properties,
                    )
                else:
                    query_txt = """{%s (first: %s, offset: %s, %s){%s}}""" % (
                        node,
                        chunk_size,
                        offset,
                        args,
                        properties,
                    )

            res = self.sub.query(query_txt)
            if "data" in res:
                records = res["data"][node]

                if len(records) < chunk_size:
                    if qsize == 999999999:
                        return total

                total["data"][node] += records  # res['data'][node] should be a list
                offset += chunk_size
            elif "error" in res:
                print(res["error"])
                if chunk_size > 1:
                    chunk_size = int(chunk_size / 2)
                    print("Halving chunk_size to: " + str(chunk_size) + ".")
                else:
                    print("Query timing out with chunk_size of 1!")
                    exit(1)
            else:
                print("Query Error: " + str(res))

            pct = int((len(total["data"][node]) / qsize) * 100)
            msg = "\tRecords retrieved: {} of {} ({}%), offset: {}, chunk_size: {}.".format(
                len(total["data"][node]), qsize, pct, offset, chunk_size
            )
            # print(msg)
            sys.stdout.write("\r" + str(msg).ljust(200, " "))

        if format == "tsv":
            df = json_normalize(total["data"][node])
            return df
        else:
            return total

    def paginate_query_new(
        self,
        node,
        project_id=None,
        props=[],
        args=None,
        chunk_size=5000,
        offset=0,
        format="json",
    ):
        """Function to paginate a query to avoid time-outs.
        Returns a json of all the records in the node.
        Args:
            node (str): The node to query.
            project_id(str): The project_id to limit the query to. Default == None.
            props(list): A list of properties in the node to return.
            args(str): Put graphQL arguments here. For example, 'with_path_to:{type:"case",submitter_id:"case-01"}', etc. Don't enclose in parentheses.
            chunk_size(int): The number of records to return per query. Default is 10000.
            offset(int): Return results with an offset; setting offset=10 will skip first 10 records.
            format(str): 'json' or 'tsv'. If set to 'tsv', function will return DataFrame and create a TSV file from it.
        Example:
            paginate_query('demographic')
        """
        props = list(set(["id", "submitter_id"] + props))
        properties = " ".join(map(str, props))

        if project_id != None:
            outname = "query_{}_{}.tsv".format(project_id, node)
            if args == None:
                query_txt = """{%s (first: %s, offset: %s, project_id:"%s"){%s}}""" % (
                    node,
                    chunk_size,
                    offset,
                    project_id,
                    properties,
                )
            else:
                query_txt = (
                    """{%s (first: %s, offset: %s, project_id:"%s", %s){%s}}"""
                    % (node, chunk_size, offset, project_id, args, properties)
                )
        else:
            outname = "query_{}.tsv".format(node)
            if args == None:
                query_txt = """{%s (first: %s, offset: %s){%s}}""" % (
                    node,
                    chunk_size,
                    offset,
                    properties,
                )
            else:
                query_txt = """{%s (first: %s, offset: %s, %s){%s}}""" % (
                    node,
                    chunk_size,
                    offset,
                    args,
                    properties,
                )

        total = {}
        total["data"] = {}
        total["data"][node] = []

        records = list(range(chunk_size))
        while len(records) == chunk_size:

            res = self.sub.query(query_txt)

            if "data" in res:
                records = res["data"][node]
                total["data"][node] += records  # res['data'][node] should be a list
                offset += chunk_size

            elif "error" in res:
                print(res["error"])
                if chunk_size > 1:
                    chunk_size = int(chunk_size / 2)
                    print("\tHalving chunk_size to: {}.".format(chunk_size))
                else:
                    print("\tQuery timing out with chunk_size of 1!")
                    exit(1)

            else:
                print("Query Error: {}".format(res))

            print("\tTotal records retrieved: {}".format(len(total["data"][node])))

        if format == "tsv":
            df = json_normalize(total["data"][node])
            df.to_csv(outname, sep="\t", index=False)
            return df
        else:
            return total

    def get_uuids_in_node(self, node, project_id):
        """
        This function returns a list of all the UUIDs of records
        in a particular node of a given project.
        """
        program, project = project_id.split("-", 1)

        try:
            res = self.paginate_query(node, project_id)
            uuids = [x["id"] for x in res["data"][node]]
        except:
            raise Gen3Error(
                "Failed to get UUIDs in node '"
                + node
                + "' of project '"
                + project_id
                + "'."
            )

        return uuids

    def list_project_files(self, project_id):
        query_txt = (
            """{datanode(first:-1,project_id: "%s") {type file_name id object_id}}"""
            % (project_id)
        )
        res = self.sub.query(query_txt)
        if len(res["data"]["datanode"]) == 0:
            print("Project " + project_id + " has no records in any data_file node.")
            return None
        else:
            df = json_normalize(res["data"]["datanode"])
            json_normalize(Counter(df["type"]))
            # guids = df.loc[(df['type'] == node)]['object_id']
            return df

    def get_uuids_for_submitter_ids(self, sids, node):
        """
        Get a list of UUIDs for a provided list of submitter_ids.
        """
        uuids = []
        count = 0
        for sid in sids:
            count += 1
            args = 'submitter_id:"{}"'.format(sid)
            res = self.paginate_query(node=node, args=args)
            recs = res["data"][node]
            if len(recs) == 1:
                uuids.append(recs[0]["id"])
            elif len(recs) == 0:
                print("No data returned for {}:\n\t{}".format(sid, res))
            print("\t{}/{}".format(count, len(sids)))
        print(
            "Finished retrieving {} uuids for {} submitter_ids".format(
                len(uuids), len(sids)
            )
        )
        return uuids

    def get_records_for_submitter_ids(self, sids, node):
        """
        Get a list of UUIDs for a provided list of submitter_ids.
        # could also use:{node(submitter_id: "xyz") {id project_id}} #
        """
        uuids = []
        pids = []
        count = 0
        for sid in sids:
            count += 1
            args = 'submitter_id:"{}"'.format(sid)
            res = self.paginate_query(node=node, args=args, props=["id", "submitter_id","project_id"])
            recs = res["data"][node]
            if len(recs) == 1:
                uuids.append(recs[0]["id"])
                pids.append(recs[0]["project_id"])
            elif len(recs) == 0:
                print("No data returned for {}:\n\t{}".format(sid, res))
            print("\t{}/{}".format(count, len(sids)))
        print(
            "Finished retrieving {} uuids for {} submitter_ids".format(
                len(uuids), len(sids)
            )
        )
        df = pd.DataFrame({'project_id':pids,'uuid':uuids,'submitter_id':sids})

        dfs = []
        for i in range(len(df)):
            sid = df.iloc[i]['submitter_id']
            pid = df.iloc[i]['project_id']
            uuid = df.iloc[i]['uuid']
            prog,proj = pid.split("-",1)
            print("({}/{}): {}".format(i+1,len(df),uuid))
            mydir = "project_uuids/{}_tsvs".format(pid)  # create the directory to store TSVs
            if not os.path.exists(mydir):
                os.makedirs(mydir)
            filename = "{}/{}_{}.tsv".format(mydir,pid,uuid)
            if os.path.isfile(filename):
                print("File previously downloaded.")
            else:
                self.sub.export_record(prog, proj, uuid, "tsv", filename)
            df1 = pd.read_csv(filename, sep="\t", header=0)
            dfs.append(df1)
        all_data = pd.concat(dfs, ignore_index=True)
        master = "master_uuids_{}.tsv".format(node)
        all_data.to_csv("{}".format(master), sep='\t',index=False)
        print("Master node TSV with {} total recs written to {}.".format(len(all_data),master))
        return all_data





    def delete_records(self, uuids, project_id, chunk_size=200, backup=False):
        """
        This function attempts to delete a list of UUIDs from a project.
        It returns a dictionary with a list of successfully deleted UUIDs,
        a list of those that failed, all the API responses, and all the error messages.

        Args:
            uuids(list): A list of the UUIDs to delete.
            project_id(str): The project to delete the IDs from.
            chunk_size(int): The number of records to delete in each API request.
            backup(str): If provided, deleted records are backed up to this filename.
        Example:
            delete_records(project_id=project_id,uuids=uuids,chunk_size=200)
        """
        program, project = project_id.split("-", 1)

        if isinstance(uuids, str):
            uuids = [uuids]

        if not isinstance(uuids, list):
            raise Gen3Error("Please provide a list of UUID(s) to delete with the 'uuid' argument.")

        if backup:
            ext = backup.split(".")[-1]
            fname = ".".join(backup.split(".")[0:-1])
            count = 0
            while path.exists(backup):
                count += 1
                backup = "{}_{}.{}".format(fname, count, ext)

            count = 0
            print("Attempting to backup {} records to delete to file '{}'.".format(len(uuids), backup))

            records = []
            for uuid in uuids:
                count += 1
                try:
                    response = self.sub.export_record(
                        program=program,
                        project=project,
                        uuid=uuid,
                        fileformat="json",
                        filename=None,
                    )
                    record = json.loads(json.dumps(response[0]))
                    records.append(record)
                    print(
                        "\tRetrieving record for UUID '{}' ({}/{}).".format(
                            uuid, count, len(uuids)
                        )
                    )
                except Exception as e:
                    print(
                        "Exception occurred during 'export_record' request: {}.".format(
                            e
                        )
                    )
                    continue

            with open(backup, "w") as backfile:
                backfile.write("{}".format(records))

        responses = []
        errors = []
        failure = []
        success = []
        retry = []
        tried = []
        results = {}

        while len(tried) < len(uuids):  # loop sorts all uuids into success or failure

            if len(retry) > 0:
                print("Retrying deletion of {} valid UUIDs.".format(len(retry)))
                list_ids = ",".join(retry)
                retry = []
            else:
                list_ids = ",".join(uuids[len(tried) : len(tried) + chunk_size])

            rurl = "{}/api/v0/submission/{}/{}/entities/{}".format(self._endpoint, program, project, list_ids)

            try:
                # print("\n\trurl='{}'\n".format(rurl)) # trouble-shooting
                # print("\n\tresp = requests.delete(rurl, auth=auth)")
                # print("\n\tprint(resp.text)")
                resp = requests.delete(rurl, auth=self._auth_provider)

            except Exception as e:
                chunk_size = int(chunk_size / 2)
                print("Exception occurred during delete request:\n\t{}.\n\tReducing chunk_size to '{}'.".format(e, chunk_size))
                continue

            if ("414 Request-URI Too Large" in resp.text or "service failure" in resp.text):
                chunk_size = int(chunk_size / 2)
                print("Service Failure. The chunk_size is too large. Reducing to '{}'".format(chunk_size))

            elif "The requested URL was not found on the server." in resp.text:
                print(
                    "\n Requested URL not found on server:\n\t{}\n\t{}".format(
                        resp, rurl
                    )
                )  # debug
                break
            else:  # the delete request got an API response
                # print(resp.text) #trouble-shooting
                output = json.loads(resp.text)
                responses.append(output)

                if output["success"]:  # 'success' == True or False in API response
                    success = list(set(success + [x["id"] for x in output["entities"]]))
                else:  # if one UUID fails to delete in the request, the entire request fails.
                    for entity in output["entities"]:
                        if entity["valid"]:  # get the valid entities from repsonse to retry.
                            retry.append(entity["id"])
                        else:
                            errors.append(entity["errors"][0]["message"])
                            failure.append(entity["id"])
                            failure = list(set(failure))
                    for error in list(set(errors)):
                        print("Error message for {} records: {}".format(errors.count(error), error))

            tried = list(set(success + failure))
            print("\tProgress: {}/{} (Success: {}, Failure: {}).".format(len(tried), len(uuids), len(success), len(failure)))

        # exit the while loop if
        results["failure"] = failure
        results["success"] = success
        results["responses"] = responses
        results["errors"] = errors
        print("\tFinished record deletion script.")
        if len(success) > 0:
            print("Successfully deleted {} records.".format(len(success)))
            self.nuked()
        return results

    def delete_node(self, node, project_id, chunk_size=200):
        """
        This function attempts to delete all the records in a particular node of a project.
        It returns the results of the delete_records function.
        """
        try:
            uuids = self.get_uuids_in_node(node, project_id)
        except:
            raise Gen3Error(
                "Failed to get UUIDs in the node '"
                + node
                + "' of project '"
                + project_id
                + "'."
            )

        if len(uuids) != 0:
            print("Attemping to delete {} records in the node '{}' of project '{}'.".format(len(uuids),node,project_id))

            try:
                results = self.delete_records(uuids, project_id, chunk_size)
                print("Successfully deleted {} records in the node '{}' of project '{}'.".format(len(results["success"]),node,project_id))

                if len(results["failure"]) > 0:
                    print("Failed to delete {} records. See results['errors'] for the error messages.".format(len(results["failure"])))

            except:
                raise Gen3Error("Failed to delete UUIDs in the node '{}' of project '{}'.".format(node,project_id))

            return results

    def get_submission_order(
        self,
        root_node="project",
        excluded_schemas=[
            "_definitions",
            "_settings",
            "_terms",
            "program",
            "project",
            "root",
            "data_release",
            "metaschema",
        ],
    ):
        """
        This function gets a data dictionary, and then it determines the submission order of nodes by looking at the links.
        The reverse of this is the deletion order for deleting projects. (Must delete child nodes before parents).
        """
        dd = self.sub.get_dictionary_all()
        schemas = list(dd)
        nodes = [k for k in schemas if k not in excluded_schemas]
        submission_order = [
            (root_node, 0)
        ]  # make a list of tuples with (node, order) where order is int
        while (
            len(submission_order) < len(nodes) + 1
        ):  # "root_node" != in "nodes", thus the +1
            for node in nodes:
                if (
                    len([item for item in submission_order if node in item]) == 0
                ):  # if the node != in submission_order
                    # print("Node: {}".format(node))
                    node_links = dd[node]["links"]
                    parents = []
                    for link in node_links:
                        if "target_type" in link:  # node = 'webster_step_second_test'
                            parents.append(link["target_type"])
                        elif "subgroup" in link:  # node = 'expression_array_result'
                            sub_links = link.get("subgroup")
                            if not isinstance(sub_links, list):
                                sub_links = [sub_links]
                            for sub_link in sub_links:
                                if "target_type" in sub_link:
                                    parents.append(sub_link["target_type"])
                    if False in [
                        i in [i[0] for i in submission_order] for i in parents
                    ]:
                        continue  # if any parent != already in submission_order, skip this node for now
                    else:  # submit this node after the last parent to submit
                        parents_order = [
                            item for item in submission_order if item[0] in parents
                        ]
                        submission_order.append(
                            (node, max([item[1] for item in parents_order]) + 1)
                        )
        return submission_order

    def delete_project(self, project_id, root_node="project", chunk_size=200, nuke_project=False):
        prog, proj = project_id.split("-", 1)
        submission_order = self.get_submission_order(root_node=root_node)
        delete_order = sorted(submission_order, key=lambda x: x[1], reverse=True)
        nodes = [i[0] for i in delete_order]
        try:
            nodes.remove("project")
        except:
            print("\n\nNo 'project' node in list of nodes.")
        for node in nodes:
            print("\n\tDeleting node '{}' from project '{}'.".format(node, project_id))
            # data = self.delete_node(
            #     node=node, project_id=project_id, chunk_size=chunk_size
            # )
            self.sub.delete_node(program=prog,project=proj,node_name=node)
        if nuke_project is True:
            try:
                data = self.sub.delete_project(program=prog, project=proj)
            except Exception as e:
                print("Couldn't delete project '{}':\n\t{}".format(project_id, e))
            if "Can not delete the project." in data:
                print("{}".format(data))
            else:
                print("Successfully deleted the project '{}'".format(project_id))
                self.nuked()
        else:
            self.nuked()
            print("\n\nSuccessfully deleted all nodes in the project '{}'.\nIf you'd like to delete the project node itself, then add the flag 'nuke_project=True'.".format(project_id))


    # Analysis Functions
    def property_counts_table(self, prop, df):
        df = df[df[prop].notnull()]
        counts = Counter(df[prop])
        df1 = pd.DataFrame.from_dict(counts, orient="index").reset_index()
        df1 = df1.rename(columns={"index": prop, 0: "count"}).sort_values(
            by="count", ascending=False
        )
        total = sum(df1["count"])
        df1["percent"] = round(100 * (df1["count"] / total), 1)

        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            print(df1.to_string(index=False))
            print("\nTotal Count: {}, Total Categories: {}".format(total, len(df1)))

        return df1

    def property_counts_by_project(self, prop, df):
        df = df[df[prop].notnull()]
        categories = list(set(df[prop]))
        projects = list(set(df["project_id"]))

        project_table = pd.DataFrame(columns=["Project", "Total"] + categories)
        project_table

        proj_counts = {}
        for project in projects:
            cat_counts = {}
            cat_counts["Project"] = project
            df1 = df.loc[df["project_id"] == project]
            total = 0
            for category in categories:
                cat_count = len(df1.loc[df1[prop] == category])
                total += cat_count
                cat_counts[category] = cat_count

            cat_counts["Total"] = total
            index = len(project_table)
            for key in list(cat_counts.keys()):
                project_table.loc[index, key] = cat_counts[key]

            project_table = project_table.sort_values(
                by="Total", ascending=False, na_position="first"
            )

        return project_table


    def plot_categorical_property(self, property, df):
        # plot a bar graph of categorical variable counts in a dataframe
        df = df[df[property].notnull()]
        N = len(df)
        categories, counts = zip(*Counter(df[property]).items())
        y_pos = np.arange(len(categories))
        plt.bar(y_pos, counts, align="center", alpha=0.5)
        # plt.figtext(.8, .8, 'N = '+str(N))
        plt.xticks(y_pos, categories)
        plt.ylabel("Counts")
        plt.title(str("Counts by " + property + " (N = " + str(N) + ")"))
        plt.xticks(rotation=90, horizontalalignment="center")
        # add N for each bar
        plt.show()

    def plot_numeric_property(self, property, df, by_project=False):
        # plot a histogram of numeric variable in a dataframe
        df = df[df[property].notnull()]
        data = list(df[property].astype(float))
        N = len(data)
        fig = sns.distplot(
            data,
            hist=False,
            kde=True,
            bins=int(180 / 5),
            color="darkblue",
            kde_kws={"linewidth": 2},
        )
        #        plt.figtext(.8, .8, 'N = '+str(N))
        plt.xlabel(property)
        plt.ylabel("Probability")
        plt.title(
            "PDF for all projects " + property + " (N = " + str(N) + ")"
        )  # You can comment this line out if you don't need title
        plt.show(fig)

        if by_project == True:
            projects = list(set(df["project_id"]))
            for project in projects:
                proj_df = df[df["project_id"] == project]
                data = list(proj_df[property].astype(float))
                N = len(data)
                fig = sns.distplot(
                    data,
                    hist=False,
                    kde=True,
                    bins=int(180 / 5),
                    color="darkblue",
                    kde_kws={"linewidth": 2},
                )
                #                plt.figtext(.8, .8, 'N = '+str(N))
                plt.xlabel(property)
                plt.ylabel("Probability")
                plt.title(
                    "PDF for " + property + " in " + project + " (N = " + str(N) + ")"
                )  # You can comment this line out if you don't need title
                plt.show(fig)

    def plot_numeric_property_by_category(
        self, numeric_property, category_property, df
    ):
        # plot a histogram of numeric variable in a dataframe
        df = df[df[numeric_property].notnull()]
        data = list(df[numeric_property])
        N = len(data)

        categories = list(set(df[category_property]))
        for category in categories:
            df_2 = df[df[category_property] == category]
            if len(df_2) != 0:
                data = list(df_2[numeric_property].astype(float))
                N = len(data)
                fig = sns.distplot(
                    data,
                    hist=False,
                    kde=True,
                    bins=int(180 / 5),
                    color="darkblue",
                    kde_kws={"linewidth": 2},
                )
                #            plt.figtext(.8, .8, 'N = '+str(N))
                plt.xlabel(numeric_property)
                plt.ylabel("Probability")
                plt.title(
                    "PDF of "
                    + numeric_property
                    + " for "
                    + category
                    + " (N = "
                    + str(N)
                    + ")"
                )  # You can comment this line out if you don't need title
                plt.show(fig)

    def plot_numeric_by_category(self, numeric_property, category_property, df):
        sns.set(style="darkgrid")
        categories = list(set(df[category_property]))

        N = 0
        for category in categories:
            subset = df[df[category_property] == category]
            N += len(subset)
            data = subset[numeric_property].dropna().astype(float)
            fig = sns.distplot(
                data,
                hist=False,
                kde=True,
                bins=3,
                kde_kws={"linewidth": 2},
                label=category,
            )

            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)

        plt.title(
            numeric_property + " by " + category_property + " (N = " + str(N) + ")"
        )  # You can comment this line out if you don't need title
        plt.show(fig)

    def plot_category_by_category(self, prop1, prop2, df):
        sns.set(style="darkgrid")
        categories, counts = zip(*Counter(df[prop1]).items())
        N = 0

        for category in categories:
            subset = df[df[prop1] == category]
            N += len(subset)
            data = subset[prop2].dropna().astype(str)

            y_pos = np.arange(len(categories))
            plt.bar(y_pos, counts, align="center", alpha=0.5)
            plt.xticks(y_pos, categories)
            plt.ylabel("Counts")
            plt.xticks(rotation=90, horizontalalignment="center")

        plt.title("{} by {} (N = {})".format(prop1, prop2, N))
        plt.show()

    def plot_top10_numeric_by_category(self, numeric_property, category_property, df):
        sns.set(style="darkgrid")
        categories = list(set(df[category_property]))

        category_means = {}
        for category in categories:
            df_2 = df[df[numeric_property].notnull()]
            data = list(
                df_2.loc[df_2[category_property] == category][numeric_property].astype(
                    float
                )
            )

            if len(data) > 5:
                category_means[category] = mean(data)

        if len(category_means) > 1:
            sorted_means = sorted(
                category_means.items(), key=operator.itemgetter(1), reverse=True
            )[0:10]
            categories_list = [x[0] for x in sorted_means]

        N = 0
        for category in categories_list:
            subset = df[df[category_property] == category]
            N += len(subset)
            data = subset[numeric_property].dropna().astype(float)
            fig = sns.distplot(
                data,
                hist=False,
                kde=True,
                bins=3,
                kde_kws={"linewidth": 2},
                label=category,
            )

            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)

        plt.title(
            numeric_property + " by " + category_property + " (N = " + str(N) + ")"
        )
        plt.show(fig)

    def plot_numeric_property_by_2_categories(
        self, numeric_property, category_property, category_property_2, df
    ):

        df = df[df[numeric_property].notnull()]
        data = list(df[numeric_property])
        N = len(data)
        categories = list(set(df[category_property]))

        for category in categories:
            df_2 = df[df[category_property] == category]
            categories_2 = list(
                set(df_2[category_property_2])
            )  # This is a list of all compounds tested for each tissue type.

            N = 0
            for category_2 in categories_2:
                subset = df_2[df_2[category_property_2] == category_2]
                N += len(subset)
                data = subset[numeric_property].dropna().astype(float)
                fig = sns.distplot(
                    data,
                    hist=False,
                    kde=True,
                    bins=3,
                    kde_kws={"linewidth": 2},
                    label=category_2,
                )

                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)

            plt.title(
                numeric_property + " for " + category + " (N = " + str(N) + ")"
            )  # You can comment this line out if you don't need title
            plt.show(fig)

    def plot_top10_numeric_property_by_2_categories(
        self, numeric_property, category_property, category_property_2, df
    ):
        df = df[df[numeric_property].notnull()]
        categories = list(set(df[category_property]))

        for category in categories:
            df_2 = df[df[category_property] == category]
            categories_2 = list(
                set(df_2[category_property_2])
            )  # This is a list of all category_property_2 values for each category_property value.

            category_2_means = {}
            for category_2 in categories_2:
                df_3 = df_2[df_2[numeric_property].notnull()]
                data = list(
                    df_3.loc[df_3[category_property_2] == category_2][
                        numeric_property
                    ].astype(float)
                )

                if len(data) > 5:
                    category_2_means[category_2] = mean(data)

            if len(category_2_means) > 1:
                sorted_means = sorted(
                    category_2_means.items(), key=operator.itemgetter(1), reverse=True
                )[0:10]
                categories_2_list = [x[0] for x in sorted_means]

                N = 0
                for category_2 in categories_2_list:
                    subset = df_2[df_2[category_property_2] == category_2]
                    N += len(subset)
                    data = subset[numeric_property].dropna().astype(float)
                    fig = sns.distplot(
                        data,
                        hist=False,
                        kde=True,
                        bins=3,
                        kde_kws={"linewidth": 2},
                        label=category_2,
                    )

                    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.0)

                plt.title(
                    numeric_property + " for " + category + " (N = " + str(N) + ")"
                )  # You can comment this line out if you don't need title
                plt.show(fig)

    def node_record_counts(self, project_id):
        query_txt = """{node (first:-1, project_id:"%s"){type}}""" % (project_id)
        res = self.sub.query(query_txt)
        df = json_normalize(res["data"]["node"])
        counts = Counter(df["type"])
        df = pd.DataFrame.from_dict(counts, orient="index").reset_index()
        df = df.rename(columns={"index": "node", 0: "count"})
        return df

    def get_data_file_tsvs(self, projects=None, remove_empty=True):
        # Download TSVs for all data file nodes in the specified projects
        # if no projects specified, get node for all projects
        if projects == None:
            projects = list(
                json_normalize(
                    self.sub.query("""{project (first:0){project_id}}""")["data"][
                        "project"
                    ]
                )["project_id"]
            )
        elif isinstance(projects, str):
            projects = [projects]
        # Make a directory for files
        mydir = "downloaded_data_file_tsvs"
        if not os.path.exists(mydir):
            os.makedirs(mydir)
        # list all data_file 'node_id's in the data model
        dnodes = list(
            set(
                json_normalize(
                    self.sub.query(
                        """{_node_type (first:-1,category:"data_file") {id}}"""
                    )["data"]["_node_type"]
                )["id"]
            )
        )
        mnodes = list(
            set(
                json_normalize(
                    self.sub.query(
                        """{_node_type (first:-1,category:"metadata_file") {id}}"""
                    )["data"]["_node_type"]
                )["id"]
            )
        )
        inodes = list(
            set(
                json_normalize(
                    self.sub.query(
                        """{_node_type (first:-1,category:"index_file") {id}}"""
                    )["data"]["_node_type"]
                )["id"]
            )
        )
        nodes = list(set(dnodes + mnodes + inodes))
        # get TSVs and return a master pandas DataFrame with records from every project
        dfs = []
        df_len = 0
        for node in nodes:
            for project in projects:
                filename = str(mydir + "/" + project + "_" + node + ".tsv")
                if os.path.isfile(filename):
                    print("\n" + filename + " previously downloaded.")
                else:
                    prog, proj = project.split("-", 1)
                    self.sub.export_node(
                        prog, proj, node, "tsv", filename
                    )  # use the gen3sdk to download a tsv for the node
                df1 = pd.read_csv(
                    filename, sep="\t", header=0
                )  # read in the downloaded TSV to append to the master (all projects) TSV
                dfs.append(df1)
                df_len += len(df1)  # Counting the total number of records in the node
                print(filename + " has " + str(len(df1)) + " records.")
                if remove_empty == True:
                    if df1.empty:
                        print("Removing empty file: " + filename)
                        cmd = ["rm", filename]  # look in the download directory
                        try:
                            output = subprocess.check_output(
                                cmd, stderr=subprocess.STDOUT
                            ).decode("UTF-8")
                        except Exception as e:
                            output = e.output.decode("UTF-8")
                            print("ERROR:" + output)
            all_data = pd.concat(dfs, ignore_index=True, sort=False)
            print(
                "\nlength of all dfs: " + str(df_len)
            )  # this should match len(all_data) below
            nodefile = str("master_" + node + ".tsv")
            all_data.to_csv(str(mydir + "/" + nodefile), sep="\t")
            print(
                "Master node TSV with "
                + str(len(all_data))
                + " total records written to "
                + nodefile
                + "."
            )  # this should match df_len above
        return all_data

    def list_guids_in_nodes(self, nodes=None, projects=None):
        # Get GUIDs for node(s) in project(s)
        if (
            nodes == None
        ):  # get all data_file/metadata_file/index_file 'node_id's in the data model
            categories = ["data_file", "metadata_file", "index_file"]
            nodes = []
            for category in categories:
                query_txt = """{_node_type (first:-1,category:"%s") {id}}""" % category
                df = json_normalize(self.sub.query(query_txt)["data"]["_node_type"])
                if not df.empty:
                    nodes = list(set(nodes + list(set(df["id"]))))
        elif isinstance(nodes, str):
            nodes = [nodes]
        if projects == None:
            projects = list(
                json_normalize(
                    self.sub.query("""{project (first:0){project_id}}""")["data"][
                        "project"
                    ]
                )["project_id"]
            )
        elif isinstance(projects, str):
            projects = [projects]
        all_guids = (
            {}
        )  # all_guids will be a nested dict: {project_id: {node1:[guids1],node2:[guids2]} }
        for project in projects:
            all_guids[project] = {}
            for node in nodes:
                guids = []
                query_txt = (
                    """{%s (first:-1,project_id:"%s") {project_id file_size file_name object_id id}}"""
                    % (node, project)
                )
                res = self.sub.query(query_txt)
                if len(res["data"][node]) == 0:
                    print(project + " has no records in node " + node + ".")
                    guids = None
                else:
                    df = json_normalize(res["data"][node])
                    guids = list(df["object_id"])
                    print(
                        project
                        + " has "
                        + str(len(guids))
                        + " records in node "
                        + node
                        + "."
                    )
                all_guids[project][node] = guids
                # nested dict: all_guids[project][node]
        return all_guids

    def get_access_token(self):
        """get your temporary access token using your credentials downloaded from the data portal
        variable <- jsonlite::toJSON(list(api_key = keys$api_key), auto_unbox = TRUE)
        auth <- POST('https://data.braincommons.org/user/credentials/cdis/access_token', add_headers("Content-Type" = "application/json"), body = variable)

        """
        access_token = self._auth_provider._get_auth_value()
        return access_token

    def download_file_endpoint(self, guid=None):
        """download files by getting a presigned-url from the "/user/data/download/<guid>" endpoint"""
        if not isinstance(guid, str):
            raise Gen3Error("Please, supply GUID as string.")

        download_url = "{}/user/data/download/{}".format(self._endpoint, guid)
        print("Downloading file from '{}'.".format(download_url))

        try:
            # get the pre-signed URL
            res = requests.get(
                download_url, auth=self._auth_provider
            )  # get the presigned URL
            file_url = json.loads(res.content)["url"]

            # extract the filename from the pre-signed url
            f_regex = re.compile(
                r".*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\/(.*)\?.*"
            )
            fmatch = f_regex.match(res.text)
            if fmatch:
                file_name = fmatch.groups()[0]
                print("\tSaving downloaded file as '{}'".format(file_name))
            else:
                file_name = guid
                print(
                    "No matching filename in the response. Saving file with GUID as filename."
                )

            # get the file and write the contents to the file_name
            res_file = requests.get(file_url)
            open("./{}".format(file_name), "wb").write(res_file.content)

        except Exception as e:
            print("\tFile '{}' failed to download: {}".format(file_name, e))

        return file_name

    def download_files_for_guids(
        self,
        guids=None,
        profile="profile",
        client="/home/jovyan/.gen3/gen3-client",
        method="endpoint",
    ):
        # Make a directory for files
        mydir = "downloaded_data_files"
        file_names = {}
        if not os.path.exists(mydir):
            os.makedirs(mydir)
        if isinstance(guids, str):
            guids = [guids]
        if isinstance(guids, list):
            for guid in guids:
                if method == "client":
                    cmd = (
                        client
                        + " download-single --filename-format=combined --no-prompt --profile="
                        + profile
                        + " --guid="
                        + guid
                    )
                    try:
                        output = subprocess.check_output(
                            cmd, stderr=subprocess.STDOUT, shell=True
                        ).decode("UTF-8")
                        try:
                            file_name = re.search(
                                "Successfully downloaded (.+)\\n", output
                            ).group(1)
                            cmd = "mv " + file_name + " " + mydir
                            try:
                                output = subprocess.check_output(
                                    cmd, stderr=subprocess.STDOUT, shell=True
                                ).decode("UTF-8")
                            except Exception as e:
                                output = e.output.decode("UTF-8")
                                print("ERROR:" + output)
                        except AttributeError:
                            file_name = ""  # apply your error handling
                        print("Successfully downloaded: " + file_name)
                        file_names[guid] = file_name
                    except Exception as e:
                        output = e.output.decode("UTF-8")
                        print("ERROR:" + output)
                elif method == "endpoint":
                    try:
                        file_name = self.download_file_endpoint(guid=guid)
                        file_names[guid] = file_name
                    except Exception as e:
                        print("Failed to download GUID {}: {}".format(guid, e))
                else:
                    print(
                        "\tPlease set method to either 'endpoint' or 'client'!".format()
                    )
        else:
            print(
                'Provide a list of guids to download: "get_file_by_guid(guids=guid_list)"'
            )
        return file_names

    # file_name = 'GSE63878_final_list_of_normalized_data.txt.gz'
    # exp.download_file_name(file_name)

    def download_file_name(
        self,
        file_name,
        node="datanode",
        project_id=None,
        props=[
            "type",
            "file_name",
            "object_id",
            "id",
            "submitter_id",
            "data_type",
            "data_format",
            "data_category",
        ],
        all=False,
    ):
        """downloads the first file that matches a query for a file_name in a node of a project"""
        args = 'file_name:"{}"'.format(file_name)
        response = self.paginate_query(
            node=node, project_id=project_id, props=props, args=args
        )  # Use the SDK to send the query and return the response

        if "data" in response:
            node = list(response["data"])[0]
            records = response["data"][node]

            if len(records) > 1 and all == False:
                print(
                    "\tWARNING - More than one record matched query for '{}' in '{}' node of project '{}'.".format(
                        file_name, node, project
                    )
                )
                print(
                    "\t\tDownloading the first file that matched the query:\n{}".format(
                        data[0]
                    )
                )

            if len(records) >= 1 and all == False:
                record = records[0]
                guid = record["object_id"]
                fname = self.download_file_endpoint(guid=guid)

            elif all == True:
                guids = [record["object_id"] for record in records]
                for guid in guids:
                    self.download_file_endpoint(guid=guid)

            return records

        else:
            print(
                "There were no records in the query for '{}' in the '{}' node of project_id '{}'".format(
                    file_name, node, project_id
                )
            )
            return response

    def get_records_for_uuids(self, uuids, project, api):
        dfs = []
        for uuid in uuids:
            # Gen3Submission.export_record("DCF", "CCLE", "d70b41b9-6f90-4714-8420-e043ab8b77b9", "json", filename="DCF-CCLE_one_record.json")
            # export_record(self, program, project, uuid, fileformat, filename=None)
            mydir = str(
                "project_uuids/" + project + "_tsvs"
            )  # create the directory to store TSVs
            if not os.path.exists(mydir):
                os.makedirs(mydir)
            filename = str(mydir + "/" + project + "_" + uuid + ".tsv")
            if os.path.isfile(filename):
                print("File previously downloaded.")
            else:
                prog, proj = project.split("-", 1)
                self.sub.export_record(prog, proj, uuid, "tsv", filename)
            df1 = pd.read_csv(filename, sep="\t", header=0)
            dfs.append(df1)
        all_data = pd.concat(dfs, ignore_index=True)
        master = str("master_uuids_" + project + ".tsv")
        all_data.to_csv(str(mydir + "/" + master), sep="\t")
        print(
            "Master node TSV with "
            + str(len(all_data))
            + " total records written to "
            + master
            + "."
        )
        return all_data

    def find_duplicate_filenames(self, node, project):
        # download the node
        df = get_node_tsvs(node, project, overwrite=True)
        counts = Counter(df["file_name"])
        count_df = pd.DataFrame.from_dict(counts, orient="index").reset_index()
        count_df = count_df.rename(columns={"index": "file_name", 0: "count"})
        dup_df = count_df.loc[count_df["count"] > 1]
        dup_files = list(dup_df["file_name"])
        dups = df[df["file_name"].isin(dup_files)].sort_values(
            by="md5sum", ascending=False
        )
        return dups

    def get_duplicates(self, nodes, projects, api):
        # Get duplicate SUBMITTER_IDs in a node, which SHOULD NEVER HAPPEN but alas it has, thus this script
        # if no projects specified, get node for all projects
        if projects == None:
            projects = list(
                json_normalize(
                    self.sub.query("""{project (first:0){project_id}}""")["data"][
                        "project"
                    ]
                )["project_id"]
            )
        elif isinstance(projects, str):
            projects = [projects]

        # if no nodes specified, get all nodes in data commons
        if nodes == None:
            nodes = sorted(
                list(
                    set(
                        json_normalize(
                            self.sub.query("""{_node_type (first:-1) {id}}""")["data"][
                                "_node_type"
                            ]
                        )["id"]
                    )
                )
            )  # get all the 'node_id's in the data model
            remove_nodes = [
                "program",
                "project",
                "root",
                "data_release",
            ]  # remove these nodes from list of nodes
            for node in remove_nodes:
                if node in nodes:
                    nodes.remove(node)
        elif isinstance(nodes, str):
            nodes = [nodes]

        pdups = {}
        for project_id in projects:
            pdups[project_id] = {}
            print("Getting duplicates in project " + project_id)
            for node in nodes:
                print("\tChecking " + node + " node")
                df = paginate_query(
                    node=node,
                    project_id=project_id,
                    props=["id", "submitter_id"],
                    chunk_size=1000,
                )
                if not df.empty:
                    counts = Counter(df["submitter_id"])
                    c = pd.DataFrame.from_dict(counts, orient="index").reset_index()
                    c = c.rename(columns={"index": "submitter_id", 0: "count"})
                    dupc = c.loc[c["count"] > 1]
                    if not dupc.empty:
                        dups = list(set(dupc["submitter_id"]))
                        uuids = {}
                        for sid in dups:
                            uuids[sid] = list(df.loc[df["submitter_id"] == sid]["id"])
                        pdups[project_id][node] = uuids
        return pdups

    def delete_duplicates(self, dups, project_id, api):

        if not isinstance(dups, dict):
            print(
                "Must provide duplicates as a dictionary of keys:submitter_ids and values:uuids; use get_duplicates function"
            )

        program, project = project_id.split("-", 1)
        failure = []
        success = []
        results = {}
        sids = list(dups.keys())
        total = len(sids)
        count = 1
        for sid in sids:
            while len(dups[sid]) > 1:
                uuid = dups[sid].pop(1)
                r = json.loads(self.sub.delete_record(program, project, uuid))
                if r["code"] == 200:
                    print(
                        "Deleted record id ("
                        + str(count)
                        + "/"
                        + str(total)
                        + "): "
                        + uuid
                    )
                    success.append(uuid)
                else:
                    print("Could not deleted record id: " + uuid)
                    print("API Response: " + r["code"])
                    failure.append(uuid)
            results["failure"] = failure
            results["success"] = success
            count += 1
        return results

    def query_records(self, node, project_id, api, chunk_size=500):
        # Using paginated query, Download all data in a node as a DataFrame and save as TSV
        schema = self.sub.get_dictionary_node(node)
        props = list(schema["properties"].keys())
        links = list(schema["links"])
        # need to get links out of the list of properties because they're handled differently in the query
        link_names = []
        for link in links:
            link_list = list(link)
            if "subgroup" in link_list:
                subgroup = link["subgroup"]
                for sublink in subgroup:
                    link_names.append(sublink["name"])
            else:
                link_names.append(link["name"])
        for link in link_names:
            if link in props:
                props.remove(link)
                props.append(str(link + "{id submitter_id}"))

        df = paginate_query(node, project_id, props, chunk_size)
        outfile = "_".join(project_id, node, "query.tsv")
        df.to_csv(outfile, sep="\t", index=False, encoding="utf-8")
        return df

    # Group entities in details into succeeded (successfully created/updated) and failed valid/invalid
    def summarize_submission(self, tsv, details, write_tsvs):
        with open(details, "r") as file:
            f = file.read().rstrip("\n")
        chunks = f.split("\n\n")
        invalid = []
        messages = []
        valid = []
        succeeded = []
        responses = []
        results = {}
        chunk_count = 1
        for chunk in chunks:
            d = json.loads(chunk)
            if "code" in d and d["code"] != 200:
                entities = d["entities"]
                response = str(
                    "Chunk "
                    + str(chunk_count)
                    + " Failed: "
                    + str(len(entities))
                    + " entities."
                )
                responses.append(response)
                for entity in entities:
                    sid = entity["unique_keys"][0]["submitter_id"]
                    if entity["valid"]:  # valid but failed
                        valid.append(sid)
                    else:  # invalid and failed
                        message = entity["errors"][0]["message"]
                        messages.append(message)
                        invalid.append(sid)
                        print("Invalid record: {}\n\tmessage: {}".format(sid, message))
            elif "code" not in d:
                responses.append("Chunk " + str(chunk_count) + " Timed-Out: " + str(d))
            else:
                entities = d["entities"]
                response = str(
                    "Chunk "
                    + str(chunk_count)
                    + " Succeeded: "
                    + str(len(entities))
                    + " entities."
                )
                responses.append(response)
                for entity in entities:
                    sid = entity["unique_keys"][0]["submitter_id"]
                    succeeded.append(sid)
            chunk_count += 1
        results["valid"] = valid
        results["invalid"] = invalid
        results["messages"] = messages
        results["succeeded"] = succeeded
        results["responses"] = responses
        submitted = succeeded + valid + invalid  # 1231 in test data
        # get records missing in details from the submission.tsv
        df = pd.read_csv(tsv, sep="\t", header=0)
        missing_df = df.loc[
            ~df["submitter_id"].isin(submitted)
        ]  # these are records that timed-out, 240 in test data
        missing = list(missing_df["submitter_id"])
        results["missing"] = missing

        # Find the rows in submitted TSV that are not in either failed or succeeded, 8 time outs in test data, 8*30 = 240 records
        if write_tsvs == True:
            print("Writing TSVs: ")
            valid_df = df.loc[
                df["submitter_id"].isin(valid)
            ]  # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
            invalid_df = df.loc[
                df["submitter_id"].isin(invalid)
            ]  # these are records that failed due to being invalid and should be reformatted
            sub_name = ntpath.basename(tsv)
            missing_file = "missing_" + sub_name
            valid_file = "valid_" + sub_name
            invalid_file = "invalid_" + sub_name
            missing_df.to_csv(missing_file, sep="\t", index=False, encoding="utf-8")
            valid_df.to_csv(valid_file, sep="\t", index=False, encoding="utf-8")
            invalid_df.to_csv(invalid_file, sep="\t", index=False, encoding="utf-8")
            print("\t" + missing_file)
            print("\t" + valid_file)
            print("\t" + invalid_file)

        return results

    def write_tsvs_from_results(self, invalid_ids, filename):
        # Read the file in as a pandas DataFrame
        f = os.path.basename(filename)
        if f.lower().endswith(".csv"):
            df = pd.read_csv(filename, header=0, sep=",", dtype=str).fillna("")
        elif f.lower().endswith(".xlsx"):
            xl = pd.ExcelFile(filename, dtype=str)  # load excel file
            sheet = xl.sheet_names[0]  # sheetname
            df = xl.parse(sheet)  # save sheet as dataframe
            converters = {
                col: str for col in list(df)
            }  # make sure int isn't converted to float
            df = pd.read_excel(filename, converters=converters).fillna("")  # remove nan
        elif filename.lower().endswith((".tsv", ".txt")):
            df = pd.read_csv(filename, header=0, sep="\t", dtype=str).fillna("")
        else:
            print("Please upload a file in CSV, TSV, or XLSX format.")
            exit(1)

        invalid_df = df.loc[
            df["submitter_id"].isin(invalid_ids)
        ]  # these are records that failed due to being invalid and should be reformatted
        invalid_file = "invalid_" + f + ".tsv"

        print("Writing TSVs: ")
        print("\t" + invalid_file)
        invalid_df.to_csv(invalid_file, sep="\t", index=False, encoding="utf-8")

        return invalid_df

    def submit_df(self, project_id, df, chunk_size=1000, row_offset=0):
        """Submit data in a pandas DataFrame."""
        df_type = list(set(df["type"]))
        df.rename(
            columns={c: c.lstrip("*") for c in df.columns}, inplace=True
        )  # remove any leading asterisks in the DataFrame column names

        # Check uniqueness of submitter_ids:
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            raise Gen3Error(
                "Warning: file contains duplicate submitter_ids. \nNote: submitter_ids must be unique within a node!"
            )

        # Chunk the file
        print("Submitting {} DataFrame with {} records.".format(df_type, len(df)))
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
                "\tChunk {} (chunk size: {}, submitted: {} of {})".format(
                    str(count),
                    str(chunk_size),
                    str(len(results["succeeded"]) + len(results["invalid"])),
                    str(len(df)),
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

            # Handle the API response
            if (
                "Request Timeout" in response
                or "413 Request Entity Too Large" in response
                or "Connection aborted." in response
                or "service failure - try again later" in response
            ):  # time-out, response != valid JSON at the moment

                print("\t Reducing Chunk Size: {}".format(response))
                results["responses"].append("Reducing Chunk Size: {}".format(response))
                timeout = True

            else:
                try:
                    json_res = json.loads(response)
                except JSONDecodeError as e:
                    print(response)
                    print(str(e))
                    raise Gen3Error("Unable to parse API response as JSON!")

                if "message" in json_res and "code" not in json_res:
                    print(json_res)  # trouble-shooting
                    print(
                        "\t No code in the API response for Chunk {}: {}".format(
                            str(count), json_res.get("message")
                        )
                    )
                    print("\t {}".format(str(json_res.get("transactional_errors"))))
                    results["responses"].append(
                        "Error Chunk {}: {}".format(str(count), json_res.get("message"))
                    )
                    results["other"].append(json_res.get("message"))

                elif "code" not in json_res:
                    print("\t Unhandled API-response: {}".format(response))
                    results["responses"].append(
                        "Unhandled API response: {}".format(response)
                    )

                elif json_res["code"] == 200:  # success

                    entities = json_res.get("entities", [])
                    print("\t Succeeded: {} entities.".format(str(len(entities))))
                    results["responses"].append(
                        "Chunk {} Succeeded: {} entities.".format(
                            str(count), str(len(entities))
                        )
                    )

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        results["succeeded"].append(sid)

                elif (
                    json_res["code"] == 400
                    or json_res["code"] == 403
                    or json_res["code"] == 404
                ):  # failure

                    entities = json_res.get("entities", [])
                    print("\tChunk Failed: {} entities.".format(str(len(entities))))
                    results["responses"].append(
                        "Chunk {} Failed: {} entities.".format(
                            str(count), str(len(entities))
                        )
                    )

                    message = ""
                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        if entity["valid"]:  # valid but failed
                            valid_but_failed.append(sid)
                        else:  # invalid and failed
                            message = str(entity["errors"])
                            results["invalid"][sid] = message
                            invalid.append(sid)
                    print(
                        "\tInvalid records in this chunk: {}, {}".format(
                            len(invalid), message
                        )
                    )

                elif json_res["code"] == 500:  # internal server error

                    print("\t Internal Server Error: {}".format(response))
                    results["responses"].append(
                        "Internal Server Error: {}".format(response)
                    )

            if (
                len(valid_but_failed) > 0 and len(invalid) > 0
            ):  # if valid entities failed bc grouped with invalid, retry submission
                chunk = chunk.loc[
                    df["submitter_id"].isin(valid_but_failed)
                ]  # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
                print(
                    "Retrying submission of valid entities from failed chunk: {} valid entities.".format(
                        str(len(chunk))
                    )
                )

            elif (
                len(valid_but_failed) > 0 and len(invalid) == 0
            ):  # if all entities are valid but submission still failed, probably due to duplicate submitter_ids. Can remove this section once the API response is fixed: https://ctds-planx.atlassian.net/browse/PXP-3065
                print("\tChunk with error:\n\n{}\n\n".format(chunk))
                print("\tUnhandled API response. Adding chunk to 'other' in results. Check for special characters or malformed links or property values.")
                results["other"].append(chunk)
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            elif timeout == False:  # get new chunk if didn't timeout
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            else:  # if timeout, reduce chunk size and retry smaller chunk
                if chunk_size >= 2:
                    chunk_size = int(chunk_size / 2)
                    end = start + chunk_size
                    chunk = df[start:end]
                    print(
                        "Retrying Chunk with reduced chunk_size: {}".format(
                            str(chunk_size)
                        )
                    )
                    timeout = False
                else:
                    raise Gen3SubmissionError(
                        "Submission is timing out. Please contact the Helpdesk."
                    )

        print("Finished data submission.")
        print("Successful records: {}".format(str(len(set(results["succeeded"])))))
        print("Failed invalid records: {}".format(str(len(results["invalid"]))))

        return results

    def submit_file(self, project_id, filename, chunk_size=30, row_offset=0, drop_props=['project_id']):
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
            xl = pd.ExcelFile(filename, dtype=str)  # load excel file
            sheet = xl.sheet_names[0]  # sheetname
            df = xl.parse(sheet)  # save sheet as dataframe
            converters = {
                col: str for col in list(df)
            }  # make sure int isn't converted to float
            df = pd.read_excel(filename, converters=converters).fillna("")  # remove nan
        elif filename.lower().endswith((".tsv", ".txt")):
            df = pd.read_csv(filename, header=0, sep="\t", dtype=str).fillna("")
        else:
            raise Gen3Error("Please upload a file in CSV, TSV, or XLSX format.")
        df.rename(
            columns={c: c.lstrip("*") for c in df.columns}, inplace=True
        )  # remove any leading asterisks in the DataFrame column names

        # Check uniqueness of submitter_ids:
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            raise Gen3Error(
                "Warning: file contains duplicate submitter_ids. \nNote: submitter_ids must be unique within a node!"
            )

        if drop_props is not None:
            if isinstance(drop_props,str):
                drop_props = [drop_props]
            elif isinstance(drop_props,list):
                for prop in drop_props:
                    if prop in df:
                        df.drop(columns=[prop],inplace=True)
            else:
                print("\n\n\tSubmit drop_props argument as a list of properties, e.g.,: drop_props=['id'].\n\n")

        # Chunk the file
        print("\nSubmitting {} with {} records.".format(filename, str(len(df))))
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
                    str(count),
                    str(chunk_size),
                    str(len(results["succeeded"]) + len(results["invalid"])),
                    str(len(df)),
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

            # Handle the API response
            if (
                "Request Timeout" in response
                or "413 Request Entity Too Large" in response
                or "Connection aborted." in response
                or "service failure - try again later" in response
            ):  # time-out, response != valid JSON at the moment

                print("\t Reducing Chunk Size: {}".format(response))
                results["responses"].append("Reducing Chunk Size: {}".format(response))
                timeout = True

            else:
                try:
                    json_res = json.loads(response)
                except JSONDecodeError as e:
                    print(response)
                    print(str(e))
                    raise Gen3Error("Unable to parse API response as JSON!")

                if "message" in json_res and "code" not in json_res:
                    print(json_res)  # trouble-shooting
                    print(
                        "\t No code in the API response for Chunk {}: {}".format(
                            str(count), json_res.get("message")
                        )
                    )
                    print("\t {}".format(str(json_res.get("transactional_errors"))))
                    results["responses"].append(
                        "Error Chunk {}: {}".format(str(count), json_res.get("message"))
                    )
                    results["other"].append(json_res.get("message"))

                elif "code" not in json_res:
                    print("\t Unhandled API-response: {}".format(response))
                    results["responses"].append(
                        "Unhandled API response: {}".format(response)
                    )

                elif json_res["code"] == 200:  # success

                    entities = json_res.get("entities", [])
                    print("\t Succeeded: {} entities.".format(str(len(entities))))
                    results["responses"].append(
                        "Chunk {} Succeeded: {} entities.".format(
                            str(count), str(len(entities))
                        )
                    )

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        results["succeeded"].append(sid)

                elif (
                    json_res["code"] == 400
                    or json_res["code"] == 403
                    or json_res["code"] == 404
                ):  # failure

                    entities = json_res.get("entities", [])
                    print("\tChunk Failed: {} entities.".format(str(len(entities))))
                    results["responses"].append(
                        "Chunk {} Failed: {} entities.".format(
                            str(count), str(len(entities))
                        )
                    )

                    message = ""
                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        if entity["valid"]:  # valid but failed
                            valid_but_failed.append(sid)
                        else:  # invalid and failed
                            message = str(entity["errors"])
                            results["invalid"][sid] = message
                            invalid.append(sid)
                    print(
                        "\tInvalid records in this chunk: {}, {}".format(
                            len(invalid), message
                        )
                    )

                elif json_res["code"] == 500:  # internal server error

                    print("\t Internal Server Error: {}".format(response))
                    results["responses"].append(
                        "Internal Server Error: {}".format(response)
                    )

            if (
                len(valid_but_failed) > 0 and len(invalid) > 0
            ):  # if valid entities failed bc grouped with invalid, retry submission
                chunk = chunk.loc[
                    df["submitter_id"].isin(valid_but_failed)
                ]  # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
                print(
                    "Retrying submission of valid entities from failed chunk: {} valid entities.".format(
                        str(len(chunk))
                    )
                )

            elif (
                len(valid_but_failed) > 0 and len(invalid) == 0
            ):  # if all entities are valid but submission still failed, probably due to duplicate submitter_ids. Can remove this section once the API response is fixed: https://ctds-planx.atlassian.net/browse/PXP-3065
                # raise Gen3Error(
                #     "Please check your data for correct file encoding, special characters, or duplicate submitter_ids or ids."
                # )
                print("\tUnhandled API response. Adding chunk to 'other' in results. Check for special characters or malformed links or property values.")
                results["other"].append(chunk)
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            elif timeout == False:  # get new chunk if didn't timeout
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            else:  # if timeout, reduce chunk size and retry smaller chunk
                if chunk_size >= 2:
                    chunk_size = int(chunk_size / 2)
                    end = start + chunk_size
                    chunk = df[start:end]
                    print(
                        "Retrying Chunk with reduced chunk_size: {}".format(
                            str(chunk_size)
                        )
                    )
                    timeout = False
                else:
                    raise Gen3SubmissionError(
                        "Submission is timing out. Please contact the Helpdesk."
                    )

        print("Finished data submission.")
        print("Successful records: {}".format(str(len(set(results["succeeded"])))))
        print("Failed invalid records: {}".format(str(len(results["invalid"]))))

        return results




    def submit_file_dry(self, project_id, filename, chunk_size=30, row_offset=0, drop_props=['project_id']):
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
            xl = pd.ExcelFile(filename, dtype=str)  # load excel file
            sheet = xl.sheet_names[0]  # sheetname
            df = xl.parse(sheet)  # save sheet as dataframe
            converters = {
                col: str for col in list(df)
            }  # make sure int isn't converted to float
            df = pd.read_excel(filename, converters=converters).fillna("")  # remove nan
        elif filename.lower().endswith((".tsv", ".txt")):
            df = pd.read_csv(filename, header=0, sep="\t", dtype=str).fillna("")
        else:
            raise Gen3Error("Please upload a file in CSV, TSV, or XLSX format.")
        df.rename(
            columns={c: c.lstrip("*") for c in df.columns}, inplace=True
        )  # remove any leading asterisks in the DataFrame column names

        # Check uniqueness of submitter_ids:
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            raise Gen3Error(
                "Warning: file contains duplicate submitter_ids. \nNote: submitter_ids must be unique within a node!"
            )

        if drop_props is not None:
            if isinstance(drop_props,str):
                drop_props = [drop_props]
            elif isinstance(drop_props,list):
                for prop in drop_props:
                    if prop in df:
                        df.drop(columns=[prop],inplace=True)
            else:
                print("\n\n\tSubmit drop_props argument as a list of properties, e.g.,: drop_props=['id'].\n\n")

        # Chunk the file
        print("\nSubmitting {} with {} records.".format(filename, str(len(df))))
        program, project = project_id.split("-", 1)
        api_url = "{}/api/v0/submission/{}/{}/_dry_run".format(self._endpoint, program, project)
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
                    str(count),
                    str(chunk_size),
                    str(len(results["succeeded"]) + len(results["invalid"])),
                    str(len(df)),
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

            # Handle the API response
            if (
                "Request Timeout" in response
                or "413 Request Entity Too Large" in response
                or "Connection aborted." in response
                or "service failure - try again later" in response
            ):  # time-out, response != valid JSON at the moment

                print("\t Reducing Chunk Size: {}".format(response))
                results["responses"].append("Reducing Chunk Size: {}".format(response))
                timeout = True

            else:
                try:
                    json_res = json.loads(response)
                except JSONDecodeError as e:
                    print(response)
                    print(str(e))
                    raise Gen3Error("Unable to parse API response as JSON!")

                if "message" in json_res and "code" not in json_res:
                    print(json_res)  # trouble-shooting
                    print(
                        "\t No code in the API response for Chunk {}: {}".format(
                            str(count), json_res.get("message")
                        )
                    )
                    print("\t {}".format(str(json_res.get("transactional_errors"))))
                    results["responses"].append(
                        "Error Chunk {}: {}".format(str(count), json_res.get("message"))
                    )
                    results["other"].append(json_res.get("message"))

                elif "code" not in json_res:
                    print("\t Unhandled API-response: {}".format(response))
                    results["responses"].append(
                        "Unhandled API response: {}".format(response)
                    )

                elif json_res["code"] == 200:  # success

                    entities = json_res.get("entities", [])
                    print("\t Succeeded: {} entities.".format(str(len(entities))))
                    results["responses"].append(
                        "Chunk {} Succeeded: {} entities.".format(
                            str(count), str(len(entities))
                        )
                    )

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        results["succeeded"].append(sid)

                elif (
                    json_res["code"] == 400
                    or json_res["code"] == 403
                    or json_res["code"] == 404
                ):  # failure

                    entities = json_res.get("entities", [])
                    print("\tChunk Failed: {} entities.".format(str(len(entities))))
                    results["responses"].append(
                        "Chunk {} Failed: {} entities.".format(
                            str(count), str(len(entities))
                        )
                    )

                    message = ""
                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        if entity["valid"]:  # valid but failed
                            valid_but_failed.append(sid)
                        else:  # invalid and failed
                            message = str(entity["errors"])
                            results["invalid"][sid] = message
                            invalid.append(sid)
                    print(
                        "\tInvalid records in this chunk: {}, {}".format(
                            len(invalid), message
                        )
                    )

                elif json_res["code"] == 500:  # internal server error

                    print("\t Internal Server Error: {}".format(response))
                    results["responses"].append(
                        "Internal Server Error: {}".format(response)
                    )

            if (
                len(valid_but_failed) > 0 and len(invalid) > 0
            ):  # if valid entities failed bc grouped with invalid, retry submission
                chunk = chunk.loc[
                    df["submitter_id"].isin(valid_but_failed)
                ]  # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
                print(
                    "Retrying submission of valid entities from failed chunk: {} valid entities.".format(
                        str(len(chunk))
                    )
                )

            elif (
                len(valid_but_failed) > 0 and len(invalid) == 0
            ):  # if all entities are valid but submission still failed, probably due to duplicate submitter_ids. Can remove this section once the API response is fixed: https://ctds-planx.atlassian.net/browse/PXP-3065
                # raise Gen3Error(
                #     "Please check your data for correct file encoding, special characters, or duplicate submitter_ids or ids."
                # )
                print("\tUnhandled API response. Adding chunk to 'other' in results. Check for special characters or malformed links or property values.")
                results["other"].append(chunk)
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            elif timeout == False:  # get new chunk if didn't timeout
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            else:  # if timeout, reduce chunk size and retry smaller chunk
                if chunk_size >= 2:
                    chunk_size = int(chunk_size / 2)
                    end = start + chunk_size
                    chunk = df[start:end]
                    print(
                        "Retrying Chunk with reduced chunk_size: {}".format(
                            str(chunk_size)
                        )
                    )
                    timeout = False
                else:
                    raise Gen3SubmissionError(
                        "Submission is timing out. Please contact the Helpdesk."
                    )

        print("Finished data submission.")
        print("Successful records: {}".format(str(len(set(results["succeeded"])))))
        print("Failed invalid records: {}".format(str(len(results["invalid"]))))

        return results



    # indexd functions:
    def query_indexd(self, limit=100, page=0, uploader=None, args=None):
        """Queries indexd with given records limit and page number.
        For example:
            records = query_indexd(api='https://icgc.bionimbus.org/',limit=1000,page=0)
            https://icgc.bionimbus.org/index/index/?limit=1000&page=0
        """
        data, records = {}, []

        if uploader == None:
            index_url = "{}/index/index/?limit={}&page={}".format(self._endpoint, limit, page)
        else:
            index_url = "{}/index/index/?limit={}&page={}&uploader={}".format(self._endpoint, limit, page, uploader)

        if args != None:
            index_url = "{}&{}".format(index_url,args)

        try:
            response = requests.get(index_url).text
            data = json.loads(response)
        except Exception as e:
            print(
                "\tUnable to parse indexd response as JSON!\n\t\t{} {}".format(
                    type(e), e
                )
            )

        if "records" in data:
            records = data["records"]
        else:
            print(
                "\tNo records found in data from '{}':\n\t\t{}".format(index_url, data)
            )

        return records

    def get_indexd(self, limit=1000, page=0, format="JSON", uploader=None, args=None):
        """get all the records in indexd
            api = "https://icgc.bionimbus.org/"
            args = lambda: None
            setattr(args, 'api', api)
            setattr(args, 'limit', 100)
            page = 0

        Usage:
            i = exp.get_indexd(format="TSV", uploader=orcid)
            i = exp.get_indexd(format="TSV", args="authz=/programs/TCIA/projects/COVID-19-AR")
        """
        if format in ["JSON", "TSV"]:
            dc_regex = re.compile(r"https:\/\/(.+)\/?$")
            dc = dc_regex.match(self._endpoint).groups()[0]
            dc = dc.strip('/')
        else:
            print("\n\n'{}' != a valid output format. Please provide a format of either 'JSON' or 'TSV'.\n\n".format(format))

        stats_url = "{}/index/_stats".format(self._endpoint)
        try:
            response = requests.get(stats_url).text
            stats = json.loads(response)
            print("Stats for '{}': {}".format(self._endpoint, stats))
        except Exception as e:
            print("\tUnable to parse indexd response as JSON!\n\t\t{} {}".format(type(e), e))

        print("Getting all records in indexd (limit: {}, starting at page: {})".format(limit, page))

        all_records = []

        done = False
        while done == False:

            records = self.query_indexd(limit=limit, page=page, uploader=uploader, args=args)
            all_records.extend(records)

            if len(records) != limit:
                print(
                    "\tLength of returned records ({}) does not equal limit ({}).".format(
                        len(records), limit
                    )
                )
                if len(records) == 0:
                    done = True

            print(
                "\tPage {}: {} records ({} total)".format(
                    page, len(records), len(all_records)
                )
            )
            page += 1

        print(
            "\t\tScript finished. Total records retrieved: {}".format(len(all_records))
        )
        now = datetime.datetime.now()
        date = "{}-{}-{}_{}.{}".format(now.year, now.month, now.day, now.minute, now.second)

        if format == "JSON":
            outname = "{}_indexd_records_{}.json".format(dc,date)
            with open(outname, "w") as output:
                output.write(json.dumps(all_records))

        if format == "TSV":
            outname = "{}_indexd_records_{}.tsv".format(dc,date)
            all_records = pd.DataFrame(all_records)
            all_records['md5sum'] = [hashes.get('md5') for hashes in all_records.hashes]
            all_records.to_csv(outname,sep='\t',index=False)

        return all_records

    def delete_indexd_records(self,irecs):
        """
        Arguments:
            irecs(list): A list of indexd records. Get with, e.g., function:
                Gen3Expansion.get_indexd(uploader="cgmeyer@uchicagp.edu")
        """
        total,count = len(irecs),0
        success,failure=[],[]
        for irec in irecs:
            count+=1
            guid = irec['did']
            rev = irec['rev']
            index_url = "{}/index/index/{}?rev={}".format(self._endpoint,guid,rev)
            access_token = self.get_token()
            headers = {
              'Content-Type': 'application/json',
              'Authorization': 'bearer {}'.format(access_token)
            }
            response = requests.delete(index_url, headers=headers)
            if response.status_code == 200:
                success.append(guid)
                print("{}/{} {}: Successfully deleted '{}'.".format(count,total,response.status_code,guid))
            else:
                failure.append(guid)
                print("{}/{} {}: Failed to delete '{}'.".format(count,total,response.status_code,guid))
        if len(success) > 0:
            print("Successfully deleted {} indexd records.".format(len(success)))
            self.nuked()
        return {'success':success,'failure':failure}


    def remove_uploader_from_indexd(self, irecs):
        """
        Arguments:
            irecs(list): A list of indexd records. Get with, e.g., function:
                Gen3Expansion.get_indexd(uploader="cgmeyer@uchicagp.edu")
        """
        total,count = len(irecs),0
        success,failure=[],[]
        for irec in irecs:
            count+=1
            guid = irec['did']
            rev = irec['rev']
            payload = {'uploader':None}
            index_url = "{}/index/index/{}?rev={}".format(self._endpoint,guid,rev)
            access_token = self.get_token()
            headers = {
              'Content-Type': 'application/json',
              'Authorization': 'bearer {}'.format(access_token)
            }
            response = requests.put(index_url, headers=headers, json=payload)
            if response.status_code == 200:
                success.append(guid)
            else:
                failure.append(guid)
            print("{}/{} {}: {}".format(count,total,response.status_code,response.text.encode('utf8')))
        return {'success':success,'failure':failure}

    def get_urls(self, guids):
        # Get URLs for a list of GUIDs
        if isinstance(guids, str):
            guids = [guids]
        if isinstance(guids, list):
            urls = {}
            for guid in guids:
                index_url = "{}/index/{}".format(self._endpoint, guid)
                output = requests.get(index_url, auth=self._auth_provider).text
                guid_index = json.loads(output)
                url = guid_index["urls"][0]
                urls[guid] = url
        else:
            print(
                "Please provide one or a list of data file GUIDs: get_urls\(guids=guid_list\)"
            )
        return urls

    def get_guids_for_file_names(self, file_names, method="indexd", match="file_name"):
        # Get GUIDs for a list of file_names
        if isinstance(file_names, str):
            file_names = [file_names]
        if not isinstance(file_names, list):
            print(
                "Please provide one or a list of data file file_names: get_guid_for_filename\(file_names=file_name_list\)"
            )
        guids = {}
        if method == "indexd":
            for file_name in file_names:
                index_url = "{}/index/index/?file_name={}".format(
                    self._endpoint, file_name
                )
                response = requests.get(index_url, auth=self._auth_provider).text
                index_record = json.loads(response)
                if len(index_record["records"]) > 0:
                    guid = index_record["records"][0]["did"]
                    guids[file_name] = guid
        elif method == "sheepdog":
            for file_name in file_names:
                if match == "file_name":
                    args = 'file_name:"{}"'.format(file_name)
                elif match == "submitter_id":
                    args = 'submitter_id:"{}"'.format(file_name)
                props = ["object_id"]
                res = self.paginate_query(node="datanode", args=args, props=props)
                recs = res["data"]["datanode"]
                if len(recs) >= 1:
                    guid = recs[0]["object_id"]
                    guids[file_name] = guid
                else:
                    print(
                        "Found no sheepdog records with {}: {}".format(
                            method, file_name
                        )
                    )
                if len(recs) > 1:
                    guids = [rec["object_id"] for rec in recs]
                    guids[file_name] = guids
                    print(
                        "Found more than 1 sheepdog record with {}: {}".format(
                            method, file_name
                        )
                    )
        else:
            print("Enter a valid method.\n\tValid methods: 'sheepdog','indexd'")
        return guids

    def get_index_for_file_names(self, file_names, format='tsv'):
        # Get GUIDs for a list of file_names
        if isinstance(file_names, str):
            file_names = [file_names]
        if not isinstance(file_names, list):
            print(
                "Please provide one or a list of data file file_names: get_guid_for_filename\(file_names=file_name_list\)"
            )
        all_records = []
        for file_name in file_names:
            print("\tGetting indexd record for {}".format(file_name))
            index_url = "{}/index/index/?file_name={}".format(self._endpoint, file_name)
            response = requests.get(index_url, auth=self._auth_provider).text
            response = json.loads(response)
            if 'records' in response:
                records = response['records']
                if len(records) == 1:
                    irec = records[0]
                else:
                    print("\tMultiple indexd records found for file_name '{}'!\n\t{}\n".format(file_name,records))
            else:
                print("\tNo indexd records found for file_name '{}'!".format(file_name))
            all_records.append(irec)


        if all_records == None:
            print("No records in the index with authz {}.".format(authz))

        elif format == "tsv":
            df = json_normalize(all_records)
            filename = "indexd_records_for_filenames.tsv"
            df.to_csv(filename, sep="\t", index=False, encoding="utf-8")
            return df

        elif format == "guids":
            guids = []
            for record in all_records:
                guids.append(record["did"])
            return guids

        else:
            return all_records

        return all_records


    def get_index_for_authz(self, authz, format="tsv",page=0,limit=100):
        # Get GUIDs for a particular project (authz)
        # https://data.bloodpac.org/index/index/?authz=/programs/bpa/projects/UAMS_P0001_T1
        # exp.get_index_for_authz(authz='/programs/bpa/projects/UAMS_P0001_T1')

        if isinstance(authz,list):
            authz=authz[0]

        all_records,records = [],[]
        done = False
        while done == False:

            index_url = "{}/index/index/?limit={}&page={}&authz={}".format(self._endpoint,limit,page,authz)
            #index_url = "{}/index/index/?limit={}&page={}&authz={}".format(api,limit,page,authz)
            response = requests.get(index_url, auth=self._auth_provider).text
            #response = requests.get(index_url, auth=auth).text
            data = json.loads(response)
            if "records" in data:
                records = data['records']
                all_records.extend(records)

                if len(records) == 0:
                    done = True

            print(
                "\tPage {}: {} records ({} total)".format(
                    page, len(records), len(all_records)
                )
            )
            page += 1

        print(
            "\t\tScript finished. Total records retrieved: {}".format(len(all_records))
        )


        # index_url = "{}/index/index/?authz={}".format(self._endpoint, authz)
        # response = requests.get(index_url, auth=self._auth_provider).text
        # records = json.loads(response)["records"]
        # data.append(records)
        #

        if all_records == None:
            print("No records in the index with authz {}.".format(authz))

        elif format == "tsv":
            df = json_normalize(all_records)
            filename = "indexd_records_for_{}.tsv".format(authz.split("/")[-1])
            df.to_csv(filename, sep="\t", index=False, encoding="utf-8")
            return df

        elif format == "guids":
            guids = []
            for record in all_records:
                guids.append(record["did"])
            return guids

        else:
            return all_records

        return all_records



    def get_index_for_acl(self, acl, format="guids",page=0,limit=100):
        # Get GUIDs for a particular project (acl)
        # https://data.bloodpac.org/index/index/?acl=UAMS_P0001_T1,bpa
        # exp.get_index_for_acl(acl='UAMS_P0001_T1,bpa')

        if isinstance(acl,list):
            acl="{},{}".format(acl[0],acl[1])

        all_records,records = [],[]
        done = False
        while done == False:

            index_url = "{}/index/index/?limit={}&page={}&acl={}".format(self._endpoint,limit,page,acl)
            #index_url = "{}/index/index/?limit={}&page={}&acl={}".format(api,limit,page,acl)
            response = requests.get(index_url, auth=self._auth_provider).text
            #response = requests.get(index_url, auth=auth).text
            data = json.loads(response)
            if "records" in data:
                records = data['records']
                all_records.extend(records)

                if len(records) == 0:
                    done = True

            print(
                "\tPage {}: {} records ({} total)".format(
                    page, len(records), len(all_records)
                )
            )
            page += 1

        print(
            "\t\tScript finished. Total records retrieved: {}".format(len(all_records))
        )

        if all_records == None:
            print("No records in the index with acl {}.".format(acl))

        elif format == "tsv":
            df = json_normalize(all_records)
            filename = "indexd_records_for_{}.tsv".format(acl)
            df.to_csv(filename, sep="\t", index=False, encoding="utf-8")
            return df

        elif format == "guids":
            guids = []
            for record in all_records:
                guids.append(record["did"])
            return guids

        else:
            return all_records

        return all_records


    def get_index_for_url(self, url):
        """Returns the indexd record for a file's storage location URL ('urls' in indexd)
        Example:
            api='https://icgc.bionimbus.org/'
            url='s3://pcawg-tcga-sarc-us/2720a2b8-3f4e-5b6e-9f74-1067a068462a'
            exp.get_index_for_url(url=url,api=api)
        """
        indexd_endpoint = "{}/index/index/".format(self._endpoint)
        indexd_query = "{}?url={}".format(indexd_endpoint, url)
        output = requests.get(indexd_query, auth=self._auth_provider).text
        response = json.loads(output)
        index_records = response["records"]
        return index_records

    def get_index_for_guids(self, guids):
        """Returns the indexd record for a GUID ('urls' in indexd)"""
        if isinstance(guids, str):
            guids = [guids]

        index_records = []
        for guid in guids:
            print(
                "\tGetting index for GUID ({}/{}): {}".format(
                    len(index_records), len(guids), guid
                )
            )
            indexd_endpoint = "{}/index/index/".format(self._endpoint)
            indexd_query = "{}{}".format(indexd_endpoint, guid)
            response = requests.get(indexd_query, auth=self._auth_provider).text
            records = json.loads(response)
            index_records.append(records)
        return index_records

    # failed = [irec for irec in irecs if irec['size'] == None]
    # failed_guids = [irec['did'] for irec in failed]

    def get_guid_for_url(self, url):
        """Return the GUID for a file's URL in indexd
        Example:
            api='https://icgc.bionimbus.org/'
            url='s3://pcawg-tcga-sarc-us/2720a2b8-3f4e-5b6e-9f74-1067a068462a'
            exp.get_guid_for_url(url=url,api=api)
        """
        index_records = self.get_index_for_url(url=url)
        if len(index_records) == 1:
            guid = index_records[0]["did"]
            return guid
        else:
            guids = []
            for index_record in index_records:
                guids.append(index_record["did"])
            return guids

    def delete_uploaded_files(self, guids):
        """
        DELETE http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/fence/master/openapis/swagger.yaml#/data/delete_data__file_id_
        Deletes all locations of a stored data file and remove its record from indexd.
        After a user uploads a data file and it is registered in indexd,
        but before it is mapped into the graph via metadata submission,
        this endpoint will delete the file from its storage locations (saved in the record in indexd)
        and delete the record in indexd.

        Args:
            guids (list): The list of GUIDs to delete.

        Examples:
            >>> Gen3Expansion.delete_uploaded_files(guids="dg.7519/fd0d91e0-87a6-4627-80b4-50d98614c560")
            >>> Gen3Expansion.delete_uploaded_files(guids=["dg.7519/fd0d91e0-87a6-4627-80b4-50d98614c560","dg.7519/bc78b25d-6203-4d5f-9257-cc6bba3fc34f"])
        """
        if isinstance(guids, str):
            guids = [guids]

        if not isinstance(guids, list):
            raise Gen3Error("Please, supply GUIDs as a list.")

        count,total = 0,len(guids)
        deleted,failed = [],[]
        for guid in guids:
            count+=1
            fence_url = "{}user/data/".format(self._endpoint)

            try:
                response = requests.delete(fence_url + guid, auth=self._auth_provider)
            except requests.exceptions.ConnectionError as e:
                raise Gen3Error(e)

            if response.status_code == 204:
                print("({}/{}) Successfully deleted GUID {}".format(count,total,guid))
                deleted.append(guid)
            else:
                print("({}/{}) Error deleting GUID {}:".format(count,total,guid))
                print(response.reason)
                failed.append(guid)
        if len(deleted) > 0:
            print("Successfully deleted {} uploaded files.".format(len(deleted)))
            self.nuked()
        return({'deleted':deleted,'failed':failed})


    # Data commons summary functions

    def t(self, var):
        vtype = type(var)
        print(vtype)
        if vtype in [dict, list]:
            print("{}".format(list(var)))
        if vtype in [str, int, float]:
            print("{}".format(var))

    def create_output_dir(self, outdir="data_summary_reports"):
        cmd = ["mkdir", "-p", outdir]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(
                "UTF-8"
            )
        except Exception as e:
            output = e.output.decode("UTF-8")
            print("ERROR:" + output)
        return outdir

    def old_list_links(self, link_list, dd):
        """return a list of indiv link names."""
        link_names = []
        for link in link_list:
            if "subgroup" in link:
                sublinks = list(link["subgroup"])
                for sublink in sublinks:
                    link_names.append(sublink["name"])
            else:
                link_names.append(link["name"])
        return link_names

    def list_links(self, node, dd):
        """return a list of indiv link names for a node"""
        link_list = dd[node]['links']
        link_names = []
        for link in link_list:
            if "subgroup" in link:
                sublinks = list(link["subgroup"])
                for sublink in sublinks:
                    link_names.append(sublink["name"])
            else:
                link_names.append(link["name"])
        return link_names


    def get_prop_type(self, node, prop, dd):
        prop_def = dd[node]["properties"][prop]
        if "type" in prop_def:
            prop_type = prop_def["type"]
            if "null" in prop_type:
                prop_type = [prop for prop in prop_type if prop != "null"][0]
        elif "enum" in prop_def:
            prop_type = "enum"
        elif "oneOf" in prop_def:
            if "type" in prop_def["oneOf"][0]:
                prop_type = prop_def["oneOf"][0]["type"]
            elif "enum" in prop_def["oneOf"][0]:
                prop_type = "enum"
        elif "anyOf" in prop_def:
            if isinstance(prop_def["anyOf"], list):
                prop_type = [x["type"] for x in prop_def["anyOf"] if "items" in x][0]
            else:
                prop_type = prop_def["anyOf"]
        else:
            print("Can't get the property type for {}!".format(shared_prop))
        return prop_type

    def summarize_dd(
        self,
        props_to_remove=["id", "submitter_id", "project_id", "created_datetime", "updated_datetime", "case_submitter_id", "state", "type", "md5sum", "object_id", "file_state", "file_size", "file_name", "projects"],
        nodes_to_remove=["root", "metaschema", "program", "project", "core_metadata_collection"],
    ):
        """Return a dict with nodes and list of properties in each node."""
        dd = self.sub.get_dictionary_all()
        nodes = []
        node_regex = re.compile(
            r"^[^_][A-Za-z0-9_]+$"
        )  # don't match _terms,_settings,_definitions, etc.)
        nodes = list(filter(node_regex.search, list(dd)))

        dds = {}
        for node in nodes:
            if node not in nodes_to_remove:
                print("\n\tnode: {}".format(node))
                dds[node] = {}
                dds[node]['title'] = dd[node]['title'].strip()
                props = list(dd[node]["properties"])

                for prop in props:
                    if prop not in props_to_remove:
                        print("\n\tprop: {}".format(prop))
                        if 'description' in dd[node]['properties'][prop]:
                            dds[node][prop] = dd[node]['properties'][prop]['description'].strip()
                        else:
                            dds[node][prop] = prop

        return dds

    def summarize_tsvs(
        self,
        tsv_dir,
        dd,
        prefix="",
        outlier_threshold=10,
        omit_props=[
            "project_id",
            "type",
            "id",
            "submitter_id",
            "case_submitter_id",
            "case_ids",
            "visit_id",
            "sample_id",
            "md5sum",
            "file_name",
            "object_id",
            "series_uid",
            "study_uid",
            "token_record_id"
        ],
        omit_nodes=["metaschema", "root", "program", "project", "data_release"],
        outdir=".",
        bin_limit=False,
        write_report=True,
        report_null=True,
    ):

        """
        Returns a summary of TSV data per project, node, and property in the specified directory "tsv_dir".
        For each property in each project, the total, non-null and null counts are returned.
        For string, enumeration and boolean properties, bins and the number of unique bins are returned.
        For integers and numbers, the mean, median, min, max, and stdev are returned.
        Outliers in numeric data are identified using "+/- stdev". The cut-off for outlier identification can be changed by raising or lowering the outlier_threshold (common setting is ~3).

        Args:
            tsv_dir(str): project_tsvs directory
            dd(dict): data dictionary of the commons result of func Gen3Submission.get_dictionary_all()
            prefix(str): Default gets TSVs from all directories ending in "_tsvs". "prefix" of the project_tsvs directories (e.g., program name of the projects: "Program_1-Project_2_tsvs"). Result of running the Gen3Expansion.get_project_tsvs() function.
            outlier_threshold(number): The upper/lower threshold for identifying outliers in numeric data is the standard deviation multiplied by this number.
            omit_props(list): Properties to omit from being summarized. It doesn't make sense to summarize certain properties, e.g., those with all unique values. May want to omit: ['sample_id','specimen_number','current_medical_condition_name','medical_condition_name','imaging_results','medication_name'].
            omit_nodes(list): Nodes in the data dictionary to omit from being summarized, e.g., program, project, data_release, root and metaschema.
            outdir(str): A directory for the output files.

        Examples:
            s = summarize_tsvs(tsv_dir='project_tsvs/',
                dd=dd)
        """

        summary = {}

        report = pd.DataFrame(
            columns=[
                "prop_id",
                "project_id",
                "node",
                "property",
                "type",
                "N",
                "nn",
                "null",
                "perc_null",
                "all_null",
                "min",
                "max",
                "median",
                "mean",
                "stdev",
                "outliers",
                "bin_number",
                "bins",
            ]
        )
        report["all_null"] = report["all_null"].astype(bool)

        dir_pattern = "{}*{}".format(prefix, "tsvs")
        project_dirs = glob.glob("{}/{}".format(tsv_dir, dir_pattern))

        nn_nodes, nn_props, null_nodes, null_props, all_prop_ids = [], [], [], [], []

        msg = "Summarizing TSVs in '{}':\n".format(tsv_dir)
        print("\n\n{}".format(msg))

        for project_dir in project_dirs:  # project_dir=project_dirs[0]

            try:
                project_id = re.search(
                    r"^{}/?([A-Za-z0-9_-]+)_tsvs$".format(tsv_dir), project_dir
                ).group(1)
            except:
                print(
                    "Couldn't extract the project_id from project_dir '{}'!".format(
                        project_dir
                    )
                )

            fpattern = "{}*{}".format(prefix, ".tsv")
            fnames = glob.glob("{}/{}".format(project_dir, fpattern))

            # msg = "\t\tFound the following {} TSVs: {}".format(len(fnames),fnames)
            # sys.stdout.write("\r" + str(msg))

            # print(fnames) # trouble-shooting
            if len(fnames) == 0:
                continue

            for (
                fname
            ) in (
                fnames
            ):  # Each node with data in the project is in one TSV file so len(fnames) is the number of nodes in the project with data.

                # print("\n\t\t{}".format(fname)) # trouble-shooting

                node_regex = (
                    re.escape(project_id) + r"_([a-zA-Z0-9_]+)\.tsv$"
                )  # node = re.search(r'^([a-zA-Z0-9_]+)-([a-zA-Z0-9]+)_([a-zA-Z0-9_]+)\.tsv$',fname).group(3)

                try:
                    node = re.search(node_regex, fname, re.IGNORECASE).group(1)

                except Exception as e:
                    print(
                        "\n\nCouldn't set node with node_regex on '{}':\n\t{}".format(
                            fname, e
                        )
                    )
                    node = fname

                df = pd.read_csv(fname, sep="\t", header=0, dtype=str)

                if df.empty:
                    print("\t\t'{}' TSV is empty. No data to summarize.\n".format(node))

                else:
                    nn_nodes.append(node)
                    prop_regex = re.compile(
                        r"^[A-Za-z0-9_]*[^.]$"
                    )  # drop the links, e.g., cases.submitter_id or diagnoses.id (matches all properties with no ".")
                    props = list(
                        filter(prop_regex.match, list(df))
                    )  # properties in this TSV to summarize
                    props = [
                        prop for prop in props if prop not in omit_props
                    ]  # omit_props=['project_id','type','id','submitter_id','case_submitter_id','case_ids','visit_id','sample_id','md5sum','file_name','object_id']

                    # msg = "\t\tTotal of {} records in '{}' TSV with {} properties.".format(len(df),node,len(props))
                    # sys.stdout.write("\r"+str(msg))

                    for prop in props:  # prop=props[0]

                        prop_name = "{}.{}".format(node, prop)
                        prop_id = "{}.{}".format(project_id, prop_name)
                        print(prop_name)

                        # because of sheepdog bug, need to inclue "None" in "null" (:facepalm:) https://ctds-planx.atlassian.net/browse/PXP-5663
                        #df.at[df[prop] == "None", prop] = np.nan

                        null = df.loc[df[prop].isnull()]
                        nn = df.loc[df[prop].notnull()]
                        perc_null = len(null)/len(df)
                        ptype = self.get_prop_type(node, prop, dd)

                        # dict for the prop's row in report dataframe
                        prop_stats = {
                            "prop_id": prop_id,
                            "project_id": project_id,
                            "node": node,
                            "property": prop,
                            "type": ptype,
                            "N": len(df),
                            "nn": len(nn),
                            "null": len(null),
                            "perc_null": perc_null,
                            "all_null": np.nan,
                            "min": np.nan,
                            "max": np.nan,
                            "median": np.nan,
                            "mean": np.nan,
                            "stdev": np.nan,
                            "outliers": np.nan,
                            "bin_number": np.nan,
                            "bins": np.nan,
                        }

                        if nn.empty:
                            null_props.append(prop_name)
                            prop_stats["all_null"] = True

                        else:
                            nn_props.append(prop_name)
                            all_prop_ids.append(prop_id)
                            prop_stats["all_null"] = False

                            msg = "\t'{}'".format(prop_id)
                            sys.stdout.write("\r" + str(msg).ljust(200, " "))

                            if ptype in ["string", "enum", "array", "boolean", "date"]:

                                if ptype == "array":

                                    all_bins = list(nn[prop])
                                    bin_list = [
                                        bin_txt.split(",") for bin_txt in list(nn[prop])
                                    ]
                                    counts = Counter(
                                        [
                                            item
                                            for sublist in bin_list
                                            for item in sublist
                                        ]
                                    )

                                elif ptype in ["string", "enum", "boolean", "date"]:

                                    counts = Counter(nn[prop])

                                df1 = pd.DataFrame.from_dict(
                                    counts, orient="index"
                                ).reset_index()
                                bins = [tuple(x) for x in df1.values]
                                bins = sorted(
                                    sorted(bins, key=lambda x: (x[0])),
                                    key=lambda x: (x[1]),
                                    reverse=True,
                                )  # sort first by name, then by value. This way, names with same value are in same order.

                                prop_stats["bins"] = bins
                                prop_stats["bin_number"] = len(bins)

                            # Get stats for numbers
                            elif ptype in ["number", "integer"]:  # prop='concentration'

                                # make a list of the data values as floats (converted from strings)
                                nn_all = nn[prop]
                                d_all = list(nn_all)

                                nn_num = (
                                    nn[prop]
                                    .apply(pd.to_numeric, errors="coerce")
                                    .dropna()
                                )
                                d = list(nn_num)

                                nn_string = nn.loc[~nn[prop].isin(list(map(str, d)))]
                                non_numbers = list(nn_string[prop])

                                if (
                                    len(d) > 0
                                ):  # if there are numbers in the data, calculate numeric stats

                                    # calculate summary stats using the float list d
                                    mean = statistics.mean(d)
                                    median = statistics.median(d)
                                    minimum = min(d)
                                    maximum = max(d)

                                    if (
                                        len(d) == 1
                                    ):  # if only one value, no stdev and no outliers
                                        std = "NA"
                                        outliers = []
                                    else:
                                        std = statistics.stdev(d)
                                        # Get outliers by mean +/- outlier_threshold * stdev
                                        cutoff = (
                                            std * outlier_threshold
                                        )  # three times the standard deviation is default
                                        lower, upper = (
                                            mean - cutoff,
                                            mean + cutoff,
                                        )  # cut-offs for outliers is 3 times the stdev below and above the mean
                                        outliers = sorted(
                                            list(
                                                set(
                                                    [
                                                        x
                                                        for x in d
                                                        if x < lower or x > upper
                                                    ]
                                                )
                                            )
                                        )

                                    # if property type is 'integer', change min, max, median to int type
                                    if ptype == "integer":
                                        median = int(median)  # median
                                        minimum = int(minimum)  # min
                                        maximum = int(maximum)  # max
                                        outliers = [
                                            int(i) for i in outliers
                                        ]  # convert outliers from float to int

                                    prop_stats["stdev"] = std
                                    prop_stats["mean"] = mean
                                    prop_stats["median"] = median
                                    prop_stats["min"] = minimum
                                    prop_stats["max"] = maximum
                                    prop_stats["outliers"] = outliers

                                # check if numeric property is mixed with strings, and if so, summarize the string data
                                if len(d_all) > len(d):

                                    msg = "\t\tFound {} string values among the {} records of prop '{}' with value(s): {}. Calculating stats only for the {} numeric values.".format(
                                        len(non_numbers),
                                        len(nn),
                                        prop,
                                        list(set(non_numbers)),
                                        len(d),
                                    )
                                    print("\n\t{}\n".format(msg))

                                    prop_stats["type"] = "mixed {},string".format(ptype)

                                    counts = Counter(nn_string[prop])
                                    df1 = pd.DataFrame.from_dict(
                                        counts, orient="index"
                                    ).reset_index()
                                    bins = [tuple(x) for x in df1.values]
                                    bins = sorted(
                                        sorted(bins, key=lambda x: (x[0])),
                                        key=lambda x: (x[1]),
                                        reverse=True,
                                    )
                                    prop_stats["bins"] = bins
                                    prop_stats["bin_number"] = len(bins)

                            else:  # If its not in the list of ptypes, exit. Need to add array handling.
                                print(
                                    "\t\t\n\n\n\nUnhandled property type!\n\n '{}': {}\n\n\n\n".format(
                                        prop_id, ptype
                                    )
                                )
                                exit()

                        if bin_limit and isinstance(prop_stats["bins"], list): # if bin_limit != False
                            prop_stats["bins"] = prop_stats["bins"][: int(bin_limit)]

                        #report = report.append(prop_stats, ignore_index=True)
                        # print("\n{}\n".format(report))
                        # print("\n{}\n".format(prop_stats))
                        pdf = pd.DataFrame.from_records([prop_stats])
                        pdf['all_null'] = pdf['all_null'].astype(bool)
                        report = pd.concat([report,pdf])


        if not report_null: # if report_null == False
            report = report.loc[report["all_null"] != True]

        # strip the col names so we can sort the report
        report.columns = report.columns.str.strip()
        report.sort_values(by=["all_null", "node", "property"], inplace=True)

        summary["report"] = report
        summary["all_prop_ids"] = all_prop_ids

        # summarize all properties
        nn_props = sorted(list(set(nn_props)))
        summary["nn_props"] = nn_props

        null_props = [prop for prop in null_props if prop not in nn_props]
        summary["null_props"] = sorted(list(set(null_props)))

        # summarize all nodes
        nn_nodes = sorted(list(set(nn_nodes)))
        summary["nn_nodes"] = nn_nodes

        dd_regex = re.compile(r"[^_][A-Za-z0-9_]+")
        dd_nodes = list(filter(dd_regex.match, list(dd)))
        dd_nodes = [node for node in dd_nodes if node not in omit_nodes]
        null_nodes = [node for node in dd_nodes if node not in nn_nodes]

        summary["null_nodes"] = null_nodes

        if write_report: # write_report == True

            self.create_output_dir(outdir=outdir)

            if "/" in tsv_dir:
                names = tsv_dir.split("/")
                names = [name for name in names if name != ""]
                name = names[-1]
            else:
                name = tsv_dir

            outname = "data_summary_{}.tsv".format(name)
            outname = "{}/{}".format(
                outdir, outname
            )  # ./data_summary_prod_tsvs_04272020.tsv

            report.to_csv(outname, sep="\t", index=False, encoding="utf-8")
            sys.stdout.write("\rReport written to file:".ljust(200, " "))
            print("\n\t{}".format(outname))

        return summary

    def compare_commons(
        self,
        reports,
        stats=[
            "type",
            "all_null",
            "N",
            "null",
            "nn",
            "min",
            "max",
            "mean",
            "median",
            "stdev",
            "bin_number",
            "bins",
            "outliers",
        ],
        write_report=True,
        outdir=".",
    ):

        """Takes two data summary reports (output of "self.write_commons_report" func), and compares the data in each.
            Comparisons are between matching project/node/property combos (aka "prop_id") in each report.
        Args:
            reports(dict): a dict of two "commons_name" : "report", where report is a pandas dataframe generated from a summary of TSV data; obtained by running write_summary_report() on the result of summarize_tsv_data().
            stats(list): the list of statistics to compare between data commons for each node/property combination
            outdir(str): directory name to save output files to, defaults to the current working dir
            write_report(boolean): If True, reports are written to files in the outdir.

        Example:
            reports = {"prod": report_0, "prep": report_1}
            c = compare_commons(reports)
        """

        dc0, dc1 = list(reports)
        r0 = copy.deepcopy(reports[dc0])
        r1 = copy.deepcopy(reports[dc1])

        r0.insert(loc=0, column="commons", value=dc0)
        r1.insert(loc=0, column="commons", value=dc1)
        report = pd.concat([r0, r1], ignore_index=True, sort=False)

        cols = list(report)
        p0 = list(r0["prop_id"])
        p1 = list(r1["prop_id"])
        prop_ids = sorted(list(set(p0 + p1)))
        total = len(prop_ids)

        dcs_stats = []
        for stat in stats:
            for dc in list(reports):
                dcs_stats.append(dc + "_" + stat)

        common_cols = [col for col in cols if col not in stats + ["commons"]]
        comparison_cols = ["comparison"] + common_cols + dcs_stats
        comparison = pd.DataFrame(columns=comparison_cols, index=prop_ids)

        prop_count = 1

        for prop_id in prop_ids:

            msg = "Comparing stats ({} of {}): '{}'".format(prop_count, total, prop_id)
            sys.stdout.write("\r" + str(msg).ljust(200, " "))

            prop_count += 1

            project_id, node, prop = prop_id.split(".")

            comparison["prop_id"][prop_id] = prop_id
            comparison["project_id"][prop_id] = project_id
            comparison["node"][prop_id] = node
            comparison["property"][prop_id] = prop

            df = report.loc[report["prop_id"] == prop_id].reset_index(drop=True)

            if len(df) == 1:
                comparison["comparison"][prop_id] = "unique"
                dc = df["commons"][0]
                for stat in stats:
                    col = "{}_{}".format(dc, stat)
                    comparison[col][prop_id] = df[stat][0]

            elif (
                len(df) == 2
            ):  # just a check that there should be 2 rows in the df (comparing prop_id stats between two different commons)
                same = []
                for (
                    stat
                ) in (
                    stats
                ):  # first, check whether any of the stats are different bw commons

                    col0 = dc0 + "_" + stat  # column name for first commons
                    col1 = dc1 + "_" + stat  # column name for second commons
                    comparison[col0][prop_id] = df.loc[df["commons"] == dc0].iloc[0][
                        stat
                    ]
                    comparison[col1][prop_id] = df.loc[df["commons"] == dc1].iloc[0][
                        stat
                    ]

                    if (
                        df[stat][0] != df[stat][1]
                    ):  # Note: if both values are "NaN" this == True; because NaN != NaN
                        if (
                            list(df[stat].isna())[0] == True
                            and list(df[stat].isna())[1] == True
                        ):  # if stats are both "NaN", data are identical
                            same.append(True)
                        else:  # if stats are different AND both values aren't "NaN", data are different
                            same.append(False)
                    else:  # if stat0 is stat1, data are identical
                        same.append(True)

                if (
                    False in same
                ):  # if any of the stats are different bw commons, tag as 'different', otherwise tagged as 'identical'
                    comparison["comparison"][prop_id] = "different"
                else:
                    comparison["comparison"][prop_id] = "identical"

            else:
                print(
                    "\n\nThe number of instances of this prop_id '{}' != 2!\n{}\n\n".format(
                        prop_id, df
                    )
                )
                return df

        # check total
        identical = comparison.loc[comparison["comparison"] == "identical"]
        different = comparison.loc[comparison["comparison"] == "different"]
        unique = comparison.loc[comparison["comparison"] == "unique"]

        if len(prop_ids) == len(identical) + len(different) + len(unique):
            msg = "All {} prop_ids in the reports were classified as having unique, identical or different data between data commons: {}.\n".format(
                len(prop_ids), list(reports)
            )
            sys.stdout.write("\r" + str(msg).ljust(200, " "))

        else:
            print("\nSome properties in the report were not classified!")

        # strip the col names so we can sort the report
        comparison.columns = comparison.columns.str.strip()
        comparison.sort_values(by=["comparison", "node", "property"], inplace=True)

        if write_report == True:

            self.create_output_dir(outdir)

            outname = "{}/comparison_{}_{}.tsv".format(outdir, dc0, dc1)
            comparison.to_csv(outname, sep="\t", index=False, encoding="utf-8")

            msg = "Comparison report written to file: {}".format(outname)
            print(msg)

        return comparison

    def get_token(self):
        with open(self._auth_provider._refresh_file, "r") as f:
            creds = json.load(f)
        token_url = "{}/user/credentials/api/access_token".format(self._endpoint)
        token = requests.post(token_url, json=creds).json()["access_token"]
        return token

    # Guppy funcs
    def guppy_query(self, node, props):

        guppy_url = "{}/guppy/graphql".format(self._endpoint)

        query = "{{ {} {{ {} }} }}".format(node, " ".join(props))

        query_json = {"query": query, "variables": None}

        print("Requesting '{}': {}".format(guppy_url, query_json))

        response = requests.post(guppy_url, json=query_json, auth=self._auth_provider)

        try:
            data = json.loads(response.text)
            return data
        except:
            print("Error querying Guppy")
            return response.text

    def guppy_aggregation(self, node, prop, format='JSON'):

        guppy_url = "{}/guppy/graphql".format(self._endpoint)

        query = "{{_aggregation {{{} {{{} {{histogram {{key count}} }}}}}}}}".format(node,prop)

        query_json = {"query": query, "variables": None}

        print("Requesting '{}': {}".format(guppy_url, query_json))

        response = requests.post(guppy_url, json=query_json, auth=self._auth_provider)

        try:
            res = json.loads(response.text)
        except:
            print("Error querying Guppy")
            return response.text
        d = res['data']['_aggregation'][node][prop]['histogram']
        if format == "JSON":
            return d
        elif format == "TSV":
            df = pd.json_normalize(d)
            return df

    def guppy_query_simple(self, query_txt, format='JSON'):

        guppy_url = "{}/guppy/graphql".format(self._endpoint)
        query_json = {"query": query_txt}
        print("Requesting '{}': {}".format(guppy_url, query_json))
        response = requests.post(guppy_url, json=query_json, auth=self._auth_provider)
        try:
            res = json.loads(response.text)
        except:
            print("Error querying Guppy")
            return response.text
        if 'data' in response:
            d = res['data']['_aggregation'][node][prop]['histogram']
            if format == "JSON":
                return d
            elif format == "TSV":
                df = pd.json_normalize(d)
                return df
        else:
            print(response.text)

    # Guppy funcs
    def guppy_download(self, node, props):

        guppy_dl = "{}guppy/download".format(self._endpoint)

        query_dl = "{{ 'type':'{0}' {{ 'fields': {1} }} }}".format(node, props)

        json_dl = {"query": query_dl, "variables": None}

        print("Requesting '{}': {}".format(guppy_dl, query_dl))

        headers = {"Authorization": "bearer " + self.get_token()}

        # response = requests.post(guppy_dl, json=json_dl, auth=self._auth_provider)
        response = requests.post(guppy_dl, json=query, headers=headers)

        try:
            data = json.loads(response.text)
            return data
        except:
            print("Error querying Guppy")
            return response.text

    def write_manifest(self, guids, filename="gen3_manifest.json"):

        with open(filename, "w") as mani:

            mani.write("[\n  {\n")

            count = 0
            for guid in guids:
                count += 1
                file_line = '    "object_id": "{}"\n'.format(guid)
                mani.write(file_line)
                if count == len(guids):
                    mani.write("  }]")
                else:
                    mani.write("  },\n  {\n")

        print("\tDone ({}/{}).".format(count, len(guids)))
        print("\tManifest written to file: {}".format(filename))
        return filename

    def list_nodes(
        self,
        excluded_schemas=[
            "_definitions",
            "_settings",
            "_terms",
            "program",
            "project",
            "root",
            "data_release",
            "metaschema",
        ],
    ):
        """
        This function gets a data dictionary, and then it determines the submission order of nodes by looking at the links.
        The reverse of this is the deletion order for deleting projects. (Must delete child nodes before parents).
        """
        dd = self.sub.get_dictionary_all()
        schemas = list(dd)
        nodes = [k for k in schemas if k not in excluded_schemas]
        return nodes

    def query_subject_ids(self, subject_id, nodes=None):
        """
        This function takes the submitter_id of a case or subject and checks for records in specified node(s) for matching value in the case_ids or subject_ids ubiquitous property.
        """
        if nodes == None:
            nodes = self.list_nodes()
        elif isinstance(nodes, str):
            nodes = [nodes]

        if "case" in nodes:
            subject_node, subject_prop = "case", "case_ids"
        else:
            subject_node, subject_prop = "subject", "subject_ids"

        # if projects == None: #if no projects specified, get node for all projects
        #     projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        # elif isinstance(projects, str):
        #     projects = [projects]

        query_args = '{}:"{}"'.format(subject_prop, subject_id)
        results = {}
        for node in nodes:
            res = self.paginate_query(
                node=node, props=["project_id", "id", "submitter_id"], args=query_args
            )
            if len(res["data"][node]) > 0:
                results[node] = res["data"][node]

        data = {}
        for node in list(results):
            # uuids = [rec['id'] for rec in results[node]]
            dfs = []
            for rec in results[node]:
                project_id = rec["project_id"]
                uuid = rec["id"]
                program, project = project_id.split("-", 1)
                rec = self.sub.export_record(
                    program=program,
                    project=project,
                    uuid=uuid,
                    fileformat="tsv",
                    filename=None,
                )
                # str_list = rec.split('\r\n')
                # headers = str_list[0].split('\t')
                # data = str_list[1].split('\t')
                # df = pd.DataFrame(data,columns=headers)
                dfs.append(pd.read_csv(StringIO(rec), sep="\t", header=0))
            df = pd.concat(dfs, ignore_index=True, sort=False)
            data[node] = df

        return data

        # visits = list(set([item for sublist in [list(set(list(df['visit_id']))) for df in data.values()] for item in sublist if not pd.isnull(item)]))

    def query_visit_ids(self, visit_ids):
        """
        This function takes visit submitter_ids and returns the visit records.
        You can extract the visit submitter_ids from the data returned from a query_subject_ids function using the following one-liner:
            visits = list(set([item for sublist in [list(set(list(df['visit_id']))) for df in data.values()] for item in sublist if not pd.isnull(item)]))
        """
        if isinstance(visit_ids, str):
            visit_ids = [visit_ids]
        if isinstance(visit_ids, list):
            visit_ids = list(set(visit_ids))
        else:
            print("Please provide one or more visit_ids!")
            return

        dfs, visit_uuids = [], []
        for visit_id in visit_ids:
            query_args = 'submitter_id:"{}"'.format(visit_id)
            res = self.paginate_query(
                node="visit", props=["project_id", "id"], args=query_args
            )
            if len(res["data"]["visit"]) > 0:
                uuid = res["data"]["visit"][0]["id"]
                project_id = res["data"]["visit"][0]["project_id"]
                program, project = project_id.split("-", 1)
                rec = self.sub.export_record(
                    program=program,
                    project=project,
                    uuid=uuid,
                    fileformat="tsv",
                    filename=None,
                )
                dfs.append(pd.read_csv(StringIO(rec), sep="\t", header=0))
        df = pd.concat(dfs, ignore_index=True, sort=False)

        return df



    def get_mds(self, data=True, limit=1000, args=None, guids=None, save=True):
        """
            Gets all the data in the metadata service for a data commons environment.
            Set data=False to get only the "guids" of the metadata entries.
        """

        if guids is None:
            if args is None:
                murl = "{}/mds/metadata?limit={}".format(self._endpoint, limit)
            else:
                murl = "{}/mds/metadata?limit={}&{}".format(self._endpoint, limit, args)

            if data is True:
                murl += "&data=True"

            print("Fetching metadata from URL: \n\t{}".format(murl))
            try:
                response = requests.get(murl)
                md = json.loads(response.text)

            except Exception as e:
                print("\tUnable to parse MDS response as JSON!\n\t\t{} {}".format(type(e), e))
                md = response.text

        else:
            if isinstance(guids,str):
                murl = "{}/mds/metadata/{}".format(self._endpoint, guids)
                print("Fetching metadata from URL: \n\t{}".format(murl))

                response = requests.get(murl)
                d = json.loads(response.text)
                md = {guids: d}

            elif isinstance(guids,list):
                md = []
                for guid in guids:
                    murl = "{}/mds/metadata/{}".format(self._endpoint, guid)
                    print("Fetching metadata from URL: \n\t{}".format(murl))
                    response = requests.get(murl)
                    md.append(json.loads(response.text))
        if save == True:
            now = datetime.datetime.now()
            date = "{}-{}-{}-{}.{}.{}".format(now.year, now.month, now.day, now.hour, now.minute, now.second)
            filename = "MDS_{}.json".format(date)

            with open(filename, 'w') as fp:
                json.dump(md, fp)

        return md


    def delete_mds(self,guids):
        """
        """
        deleted,failed = [],[]
        if isinstance(guids, str):
            guids = [guids]
        if not isinstance(guids,list):
            print("\n\tPlease submit GUIDs as a list.")

        count = 0
        total = len(guids)
        for guid in guids:
            count += 1
            mds_api = "{}/mds/metadata/{}".format(self._endpoint, guid)
            res = requests.delete(mds_api,auth=self._auth_provider)
            print(res.text)

            if res.status_code == 200:
                deleted.append(guid)
                print("({}/{}) Deleteted '{}' from MDS.".format(count, total, guid))
            else:
                failed.append(guid)
                print("({}/{}) FAILED to delete '{}' from MDS.".format(count, total, guid))
        if len(deleted) > 0:
            print("Successfully deleted {} metadata records.".format(len(deleted)))
            self.nuked()
        return {"deleted":deleted,"failed":failed}


    def submit_mds(self, mds):
        """
        Submit metadata to the metadata service (MDS) API.
        """
        submitted,failed = [],[]
        guids = list(mds)
        total = len(guids)
        count = 0
        for guid in guids:
            count+=1
            print("\n\tPosting '{}' to metadata service".format(guid))
            mds_api = "{}/mds/metadata/{}".format(self._endpoint, guid)
            res = requests.post(mds_api, json=mds[guid], auth=self._auth_provider)

            if res.status_code > 199 and res.status_code < 300:
                submitted.append(guid)
                print("({}/{}) Submitted '{}' to MDS.".format(count, total, guid))
            else:
                failed.append(guid)
                print("({}/{}) FAILED to submit '{}' to MDS.".format(count, total, guid))
                print("\n\t\t{}".format(res.text))

        return {"submitted":submitted, "failed":failed}



    def update_mds(self, mds):
        """
        Submit metadata to the metadata service (MDS) API.
        https://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/metadata-service/master/docs/openapi.yaml#/Maintain/update_metadata_metadata__guid__put
        """
        submitted,failed = [],[]
        guids = list(mds)
        total = len(guids)
        count = 0
        for guid in guids:
            count+=1
            print("\n\tPosting '{}' to metadata service".format(guid))
            mds_api = "{}/mds/metadata/{}".format(self._endpoint, guid)
            res = requests.put(mds_api, json=mds[guid], auth=self._auth_provider)

            if res.status_code > 199 and res.status_code < 300:
                submitted.append(guid)
                print("({}/{}) Submitted '{}' to MDS.".format(count, total, guid))
            else:
                failed.append(guid)
                print("({}/{}) FAILED to submit '{}' to MDS.".format(count, total, guid))
                print("\n\t\t{}".format(res.text))

        return {"submitted":submitted, "failed":failed}

####################################################################################
### Functions for MIDRC Pre-ingestion QC of new batches received from data submitters
####################################################################################
    def sort_batch_tsvs(self,batch,batch_dir):
        """
        Sorts the TSVs provided by a MIDRC data submitter into manifests and node submission TSVs.

        Args:
            batch(str): the name of the batch, e.g., "RSNA_20230303"
            batch_dir(str): the full path of the local directory where the batch TSVs are located.
        """
        tsvs = []
        for file in os.listdir(batch_dir):
            if file.endswith(".tsv"):
                tsvs.append(os.path.join(batch_dir, file))

        nodes = self.get_submission_order()
        nodes = [i[0] for i in nodes]

        node_tsvs = {}
        clinical_manifests,image_manifests = [],[]
        other_tsvs,nomatch_tsvs = [],[]
        node_regex = r".*/(\w+)_{}\.tsv".format(batch)

        for tsv in tsvs:
            print(tsv)
            if 'manifest' in tsv:
                if 'clinical' in tsv:
                    clinical_manifests.append(tsv)
                elif 'image' in tsv or 'imaging' in tsv:
                    image_manifests.append(tsv)
            else:
                match = re.findall(node_regex, tsv, re.M)
                print(match)

                if not match:
                    nomatch_tsvs.append(tsv)
                else:
                    node = match[0]
                    if node in nodes:
                        #node_tsvs.append({node:tsv})
                        node_tsvs[node] = tsv
                    elif node + "_file" in nodes:
                        #node_tsvs.append({"{}_file".format(node):tsv})
                        node_tsvs["{}_file".format(node)] = tsv
                    else:
                        other_tsvs.append({node:tsv})
        batch_tsvs = {"batch":batch,
                      "node_tsvs":node_tsvs,
                      "image_manifests":image_manifests,
                      "clinical_manifests":clinical_manifests,
                      "other_tsvs":other_tsvs,
                      "nomatch_tsvs":nomatch_tsvs}
        return batch_tsvs

    def check_case_ids(self,df,node,cids):
        """
        Check that all case IDs referenced across dataset are in case TSV; "cids" = "case_ids"

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            cids(list): the list of case IDs provided in the batch case TSV
        """
        errors = []
        extra_cids = []
        if node != 'case':
            if "case_ids" in df:
                df_cids = list(set(df["case_ids"]))
            elif "cases.submitter_id" in df:
                df_cids = list(set(df["cases.submitter_id"]))
            else:
                error = "Didn't find any case IDs in the {} TSV!".format(node)
                print(error)
                errors.append(error)
                df_cids = []
            #print("Found {} case IDs in the {} TSV.".format(len(cids),node_id))
            extra_cids = list(set(df_cids).difference(cids))

            if len(extra_cids) > 0:
                error = "{} TSV contains {} case IDs that are not present in the case TSV!\n\t{}\n\n".format(node,len(extra_cids),extra_cids)
                print(error)
                errors.append(error)

        return errors

    def check_type_field(self,df,node):
        """
        Check that the type of all values for properties in a node submission TSV match the data dictionary type

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        if not 'type' in df:
            error = "{} TSV does not have 'type' header!".format(node)
            print(error)
            errors.append(error)
        else:
            if not list(set(df.type))[0]==node:
                error = "{} TSV does not have correct 'type' field.".format(node)
                print(error)
                errors.append(error)
        return errors

    def check_submitter_id(self,df,node):
        """
        Check that the submitter_id column is complete and doesn't contain duplicates.
        "sids" is short for "submitter_ids".

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        if not 'submitter_id' in df:
            error = "{} TSV does not have 'submitter_id' header!".format(node)
            print(error)
            errors.append(error)
        else:
            sids = list(set(df.submitter_id))
            if not len(sids)==len(df):
                error = "{} TSV does not have unique submitter_ids! Submitter_ids: {}, TSV Length: {}".format(node,len(sids),len(df))
                print(error)
                errors.append(error)
        return errors

    def check_links(self,df,node,dd):
        """
        Check whether link headers are provided in a node submission TSV
        In many cases, node TSVs simply link to the case node, and submitters just provide the "case_ids" column, but we'll check anyways.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            dd(dictionary): the data dictionary being used, get with Gen3Submission.get_dictionary_all()
        """
        errors = []
        links = self.list_links(node, dd)
        if "core_metadata_collections" in links:
            links.remove("core_metadata_collections")
        if "core_metadata_collections.submitter_id" in links:
            links.remove("core_metadata_collections.submitter_id")
        for link in links:
            link_col = "{}.submitter_id".format(link)
            if link_col not in df:
                error = "'{}' link header not found in '{}' TSV.".format(link_col,node)
                print(error) # this is not necessarily an error, as some links may be optional, but must have at least 1 link
                errors.append(error)
        return errors

    # 4) special characters
    def check_special_chars(self,node,batch_tsvs): # probably need to add more types of special chars to this
        """
        Check for special characters that aren't compatible with Gen3's sheepdog submission service.

        Args:
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        filename = batch_tsvs["node_tsvs"][node]
        with open(filename, "rb") as tsv_file:
            lns = tsv_file.readlines()
            count = 0
            for ln in lns:
                count+=1
                if b"\xe2" in ln:
                    error = "{} TSV has special char in line {}: {}".format(node,count,ln)
                    print(error)
                    errors.append(error)
        return errors

    def check_required_props(self,
        df,
        node,
        dd,
        exclude_props = [ # submitters don't provide these properties, so remove them from QC check
            # case props not provided by submitters
            "datasets.submitter_id",
            "token_record_id",
            "linked_external_data",
            #series_file props not provided by submitters
            "file_name",
            "md5sum",
            "file_size",
            "object_id",
            "storage_urls",
            "core_metadata_collections.submitter_id",
            "core_metadata_collections",
            "associated_ids",
            #imaging_study props not provided by submitters
            "loinc_code",
            "loinc_system",
            "loinc_contrast",
            "loinc_long_common_name",
            "loinc_method",
            "days_from_study_to_neg_covid_test",
            "days_from_study_to_pos_covid_test"
        ]
    ):
        """
        Check whether all required properties for a node are provided in the submission TSV.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            dd(dictionary): the data dictionary being used, get with Gen3Submission.get_dictionary_all()
        """
        errors = []
        links = self.list_links(node, dd)
        any_na = df.columns[df.isna().any()].tolist()
        required_props = list(set(dd[node]['required']).difference(links).difference(exclude_props))
        for prop in required_props:
            if prop not in df:
                error = "{} TSV does not have required property header '{}'!".format(node,prop)
                print(error)
                errors.append(error)
            elif prop in any_na:
                error = "{} TSV does not have complete data for required property '{}'!".format(node,prop)
                print(error)
                errors.append(error)
        return errors

    def check_completeness(self,df,node):
        """
        Report on whether any properties in column headers have all NA/null values.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        all_na = df.columns[df.isna().all()].tolist()
        if len(all_na) > 0:
            error = "'{}' TSV has all NA values for these properties: {}".format(node,all_na)
            print(error)
            errors.append(error)
        return errors

    # 7) prop types
    def check_prop_types(self,
        df,
        node,
        dd,
        exclude_props = [ # submitters don't provide these properties, so remove them from QC check
            # case props not provided by submitters
            "datasets.submitter_id",
            "token_record_id",
            "linked_external_data",
            #series_file props not provided by submitters
            "file_name",
            "md5sum",
            "file_size",
            "object_id",
            "storage_urls",
            "core_metadata_collections.submitter_id",
            "core_metadata_collections",
            "associated_ids",
            #imaging_study props not provided by submitters
            "loinc_code",
            "loinc_system",
            "loinc_contrast",
            "loinc_long_common_name",
            "loinc_method",
            "days_from_study_to_neg_covid_test",
            "days_from_study_to_pos_covid_test"
        ]
    ):
        """
        Check that the types of properties match their values.

        Args:
            df(pandas DataFrame): the DataFrame of a node submission TSV read into pandas
            node(str): the name of the node (node ID) being checked
            dd(dictionary): the data dictionary being used, get with Gen3Submission.get_dictionary_all()
        """
        errors = []
        all_na = df.columns[df.isna().all()].tolist()
        links = self.list_links(node, dd)
        required_props = list(set(dd[node]['required']).difference(links).difference(exclude_props))
        if all_na == None:
            props = list(set(dd[node]['properties']).difference(links).difference(required_props).difference(dd[node]['systemProperties']).difference(exclude_props))
        else:
            props = list(set(dd[node]['properties']).difference(links).difference(required_props).difference(dd[node]['systemProperties']).difference(exclude_props).difference(all_na))
        for prop in props:
            if prop in df:
                if 'type' in dd[node]['properties'][prop]:
                    etype = dd[node]['properties'][prop]['type'] # expected type
                    if etype == 'array':
                        if 'items' in dd[node]['properties'][prop]:
                            etype = dd[node]['properties'][prop]['items']
                            if 'type' in dd[node]['properties'][prop]['items']:
                                etype = dd[node]['properties'][prop]['items']['type']

                    d = df[prop].dropna()
                    if etype == 'integer':
                        try:
                            d = d.astype(int)
                        except Exception as e:
                            error = "'{}' prop should be integer, but has non-integer values: {}".format(prop,e)
                            print(error)
                            errors.append(error)
                    elif etype == 'number':
                        try:
                            d = d.astype(float)
                        except Exception as e:
                            error = "'{}' prop should be integer, but has non-integer values: {}".format(prop,e)
                            print(error)
                            errors.append(error)
                    elif etype == 'boolean':
                        vals = list(set(d))
                        wrong_vals = list(set(vals).difference(['True','False','true','false','TRUE','FALSE']))
                        if len(wrong_vals) > 0:
                            error = "'{}' property has incorrect boolean values: {}".format(prop,wrong_vals)
                            print(error)
                            errors.append(error)
                    else:
                        d = d.convert_dtypes(infer_objects=True, convert_string=True, convert_integer=True, convert_boolean=True, convert_floating=True)
                        #itype = d.dtypes[prop] # inferred type
                        itype = d.dtype # inferred type
                        # if itype == 'Int64':
                        #     itype = 'integer'
                        if not etype == itype:
                            error = "'{}' property has inferred type '{}' and not the expected type: '{}'".format(prop,itype,etype)
                            print(error)
                            errors.append(error)

                    # to do: Check for min/max of number/int properties
                    if 'minimum' in dd[node]["properties"][prop]: #
                        min = dd[node]["properties"][prop]['minimum']
                        #for each value of d, are any less than min or greater than max

                elif 'enum' in dd[node]['properties'][prop]:
                    enums = dd[node]['properties'][prop]['enum']
                    vals = list(set(df[prop].dropna()))
                    wrong_vals = list(set(vals).difference(enums))
                    if len(wrong_vals) > 0:
                        error = "'{}' property has incorrect enum values: {}".format(prop,wrong_vals)
                        print(error)
                        errors.append(error)

            else:
                error = "'{}' property in dictionary is not in the '{}' TSV.".format(prop,node)
                print(error)
                errors.append(error)

        # check that columns in TSV are correctly named and present in data dictionary for that node
        df_props = list(df)
        extra_props = list(set(df_props).difference(list(set(dd[node]['properties']))))
        for link in links:
            if link in extra_props:
                extra_props.remove(link)
            alt_link = link + ".submitter_id"
            if alt_link in extra_props:
                extra_props.remove(alt_link)
        if len(extra_props) > 0:
            error = "'{}' properties in the {} TSV not in the data dictionary.".format(extra_props,node)
            print(error)
            errors.append(error)
        errors = list(set(errors))
        return errors

    def check_dry_submit(self,node):
        """
        Attempt to dry submit a node submission TSV

        Args:
            node(str): the name of the node (node ID) being checked
        """
        errors = []
        if node in batch_tsvs["node_tsvs"]:
            filename = batch_tsvs["node_tsvs"][node]
            if not filename:
                print("Couldn't find the {} TSV!".format(node))
            else:
                try:
                    d = self.submit_file_dry(project_id=pid,filename=filename,chunk_size=1000)
                except Exception as e:
                    error = "'{}' TSV dry run submission failed: {}".format(node,e)
                    print(error)
                    errors.append(error)
        return errors

    def read_image_manifests(self,
        image_manifests,
        cols = ['md5sum',
                'storage_urls',
                'file_size',
                'case_ids',
                'study_uid',
                'series_uid',
                'file_name']):
        """
        Reads in and concatenates image manifests if multiple manifests are provided for a batch.

        Args:
            image_manifests(list): a list of all TSV files matching the format of an image manifest in a batch of TSVs
            cols(list): the columns required in the image manifest for the packaging script to run properly.
        """
        idf = pd.DataFrame(columns=cols)
        for image_manifest in image_manifests:
            try:
                df = pd.read_csv(image_manifest,sep='\t',header=0,dtype=str)
                df = df[cols]
                idf = pd.concat([idf,df])
            except:
                print("Couldn't read in the image manifests!")
        return idf

    def check_image_manifest(self,
        idf,
        cids,
        cols = ['md5sum',
                'storage_urls',
                'file_size',
                'case_ids',
                'study_uid',
                'series_uid',
                'file_name']):
        """
        Check for missing required columns in an image manifest.

        Args:
            idf(DataFrame): the master imaging manifest DataFrame; obtained by running Gen3Expansion.read_image_manifests()
            cids(list): list of all the case IDs from the case TSV
            cols(list): the columns required in the image manifest for the packaging script to run properly.
        """
        errors = []
        for col in cols:
            missing = len(idf[idf[col].isnull()])
            if missing > 0:
                error = "'{}' values issing for image manifest column '{}'.".format(len(missing),col)
                print(error)
                errors.append(error)
        if "case_ids" in idf:
            icids = list(set(idf["case_ids"]))
            extra_cids = list(set(icids).difference(cids))
            if len(extra_cids) > 0:
                error = "The image manifest TSV contains {} case IDs that are not present in the case TSV!".format(len(extra_cids))
                print(error)
                errors.append(error)
        else:
            error = "'case_ids' column missing from image manifest!"
            print(error)
            errors.append(error)
        return errors

    def summarize_new_batch(
        self,
        batch_tsvs,
        dd,
        outlier_threshold=10,
        omit_props=[
            "project_id",
            "type",
            "id",
            "submitter_id",
            "case_submitter_id",
            "case_ids",
            "visit_id",
            "sample_id",
            "md5sum",
            "file_name",
            "object_id",
            "series_uid",
            "study_uid",
            "token_record_id"
        ],
        omit_nodes=["metaschema", "root", "program", "project", "data_release"],
        outdir=".",
        bin_limit=10,
        write_report=True,
        report_null=True,
    ):

        """
        Summarizes a batch of MIDRC submission TSVs.
        For each property in each batch submission TSVs, the total, non-null and null counts are returned.
        For string, enumeration and boolean properties, bins and the number of unique bins are returned.
        For integers and numbers, the mean, median, min, max, and stdev are returned.
        Outliers in numeric data are identified using "+/- stdev". The cut-off for outlier identification can be changed by raising or lowering the outlier_threshold (common setting is ~3).

        Args:
            batch_tsvs(dict): dictionary of batch TSV names and filenames for a batch; output of "Gen3Expansion.sort_batch_tsvs()" script
            dd(dict): data dictionary of the commons result of func Gen3Submission.get_dictionary_all()
            outlier_threshold(number): The upper/lower threshold for identifying outliers in numeric data is the standard deviation multiplied by this number.
            omit_props(list): Properties to omit from being summarized. It doesn't make sense to summarize certain properties, e.g., those with all unique values. May want to omit: ['sample_id','specimen_number','current_medical_condition_name','medical_condition_name','imaging_results','medication_name'].
            omit_nodes(list): Nodes in the data dictionary to omit from being summarized, e.g., program, project, data_release, root and metaschema.
            outdir(str): A directory for the output files.

        Examples:
            s = summarize_tsvs(batch_tsvs=batch_tsvs,
                dd=dd,bin_limit=10)
        """

        summary = {}

        report = pd.DataFrame(
            columns=[
                #"prop_id",
                #"project_id",
                "node",
                "property",
                "type",
                "N",
                "nn",
                "null",
                "perc_null",
                "all_null",
                "min",
                "max",
                "median",
                "mean",
                "stdev",
                "outliers",
                "bin_number",
                "bins",
            ]
        )
        report["all_null"] = report["all_null"].astype(bool)

        nn_nodes, nn_props, null_nodes, null_props = [], [], [], []
        #all_prop_ids = []

        for node in batch_tsvs["node_tsvs"]:
            filename = batch_tsvs["node_tsvs"][node]
            df = pd.read_csv(filename, sep="\t", header=0, dtype=str)

            if df.empty:
                print("\t\t'{}' TSV is empty. No data to summarize.\n".format(node))

            else:
                nn_nodes.append(node)
                prop_regex = re.compile(
                    r"^[A-Za-z0-9_]*[^.]$"
                )  # drop the links, e.g., cases.submitter_id or diagnoses.id (matches all properties with no ".")
                props = list(
                    filter(prop_regex.match, list(df))
                )  # properties in this TSV to summarize
                props = [
                    prop for prop in props if prop not in omit_props
                ]  # omit_props=['project_id','type','id','submitter_id','case_submitter_id','case_ids','visit_id','sample_id','md5sum','file_name','object_id']

                # msg = "\t\tTotal of {} records in '{}' TSV with {} properties.".format(len(df),node,len(props))
                # sys.stdout.write("\r"+str(msg))

                for prop in props:  # prop=props[0]

                    prop_name = "{}.{}".format(node, prop)
                    #prop_id = "{}.{}".format(project_id, prop_name)
                    print(prop_name)

                    # because of sheepdog bug, need to inclue "None" in "null" (:facepalm:) https://ctds-planx.atlassian.net/browse/PXP-5663
                    #df.at[df[prop] == "None", prop] = np.nan

                    null = df.loc[df[prop].isnull()]
                    nn = df.loc[df[prop].notnull()]
                    perc_null = len(null)/len(df)
                    ptype = self.get_prop_type(node, prop, dd)

                    # dict for the prop's row in report dataframe
                    prop_stats = {
                        #"prop_id": prop_id,
                        #"project_id": project_id,
                        "node": node,
                        "property": prop,
                        "type": ptype,
                        "N": len(df),
                        "nn": len(nn),
                        "null": len(null),
                        "perc_null": perc_null,
                        "all_null": np.nan,
                        "min": np.nan,
                        "max": np.nan,
                        "median": np.nan,
                        "mean": np.nan,
                        "stdev": np.nan,
                        "outliers": np.nan,
                        "bin_number": np.nan,
                        "bins": np.nan,
                    }

                    if nn.empty:
                        null_props.append(prop_name)
                        prop_stats["all_null"] = True

                    else:
                        nn_props.append(prop_name)
                        #all_prop_ids.append(prop_id)
                        prop_stats["all_null"] = False

                        msg = "\t'{}'".format(prop_name)
                        sys.stdout.write("\r" + str(msg).ljust(200, " "))

                        if ptype in ["string", "enum", "array", "boolean", "date"]:

                            if ptype == "array":

                                all_bins = list(nn[prop])
                                bin_list = [
                                    bin_txt.split(",") for bin_txt in list(nn[prop])
                                ]
                                counts = Counter(
                                    [
                                        item
                                        for sublist in bin_list
                                        for item in sublist
                                    ]
                                )

                            elif ptype in ["string", "enum", "boolean", "date"]:

                                counts = Counter(nn[prop])

                            df1 = pd.DataFrame.from_dict(
                                counts, orient="index"
                            ).reset_index()
                            bins = [tuple(x) for x in df1.values]
                            bins = sorted(
                                sorted(bins, key=lambda x: (x[0])),
                                key=lambda x: (x[1]),
                                reverse=True,
                            )  # sort first by name, then by value. This way, names with same value are in same order.

                            prop_stats["bins"] = bins
                            prop_stats["bin_number"] = len(bins)

                        # Get stats for numbers
                        elif ptype in ["number", "integer"]:  # prop='concentration'

                            # make a list of the data values as floats (converted from strings)
                            nn_all = nn[prop]
                            d_all = list(nn_all)

                            nn_num = (
                                nn[prop]
                                .apply(pd.to_numeric, errors="coerce")
                                .dropna()
                            )
                            d = list(nn_num)

                            nn_string = nn.loc[~nn[prop].isin(list(map(str, d)))]
                            non_numbers = list(nn_string[prop])

                            if (
                                len(d) > 0
                            ):  # if there are numbers in the data, calculate numeric stats

                                # calculate summary stats using the float list d
                                mean = statistics.mean(d)
                                median = statistics.median(d)
                                minimum = min(d)
                                maximum = max(d)

                                if (
                                    len(d) == 1
                                ):  # if only one value, no stdev and no outliers
                                    std = "NA"
                                    outliers = []
                                else:
                                    std = statistics.stdev(d)
                                    # Get outliers by mean +/- outlier_threshold * stdev
                                    cutoff = (
                                        std * outlier_threshold
                                    )  # three times the standard deviation is default
                                    lower, upper = (
                                        mean - cutoff,
                                        mean + cutoff,
                                    )  # cut-offs for outliers is 3 times the stdev below and above the mean
                                    outliers = sorted(
                                        list(
                                            set(
                                                [
                                                    x
                                                    for x in d
                                                    if x < lower or x > upper
                                                ]
                                            )
                                        )
                                    )

                                # if property type is 'integer', change min, max, median to int type
                                if ptype == "integer":
                                    median = int(median)  # median
                                    minimum = int(minimum)  # min
                                    maximum = int(maximum)  # max
                                    outliers = [
                                        int(i) for i in outliers
                                    ]  # convert outliers from float to int

                                prop_stats["stdev"] = std
                                prop_stats["mean"] = mean
                                prop_stats["median"] = median
                                prop_stats["min"] = minimum
                                prop_stats["max"] = maximum
                                prop_stats["outliers"] = outliers

                            # check if numeric property is mixed with strings, and if so, summarize the string data
                            if len(d_all) > len(d):

                                msg = "\t\tFound {} string values among the {} records of prop '{}' with value(s): {}. Calculating stats only for the {} numeric values.".format(
                                    len(non_numbers),
                                    len(nn),
                                    prop,
                                    list(set(non_numbers)),
                                    len(d),
                                )
                                print("\n\t{}\n".format(msg))

                                prop_stats["type"] = "mixed {},string".format(ptype)

                                counts = Counter(nn_string[prop])
                                df1 = pd.DataFrame.from_dict(
                                    counts, orient="index"
                                ).reset_index()
                                bins = [tuple(x) for x in df1.values]
                                bins = sorted(
                                    sorted(bins, key=lambda x: (x[0])),
                                    key=lambda x: (x[1]),
                                    reverse=True,
                                )
                                prop_stats["bins"] = bins
                                prop_stats["bin_number"] = len(bins)

                        else:  # If its not in the list of ptypes, exit. Need to add array handling.
                            print(
                                "\t\t\n\n\n\nUnhandled property type!\n\n '{}': {}\n\n\n\n".format(
                                    prop_name, ptype
                                )
                            )
                            exit()

                    if bin_limit and isinstance(prop_stats["bins"], list): # if bin_limit != False
                        prop_stats["bins"] = prop_stats["bins"][: int(bin_limit)]

                    #report = report.append(prop_stats, ignore_index=True)
                    # print("\n{}\n".format(report))
                    # print("\n{}\n".format(prop_stats))
                    pdf = pd.DataFrame.from_records([prop_stats])
                    pdf['all_null'] = pdf['all_null'].astype(bool)
                    report = pd.concat([report,pdf])


        if not report_null: # if report_null == False
            report = report.loc[report["all_null"] != True]

        # strip the col names so we can sort the report
        report.columns = report.columns.str.strip()
        report.sort_values(by=["all_null", "node", "property"], inplace=True)

        summary["report"] = report
        #
        #summary["all_prop_ids"] = all_prop_ids

        # summarize all properties
        nn_props = sorted(list(set(nn_props)))
        summary["nn_props"] = nn_props

        null_props = [prop for prop in null_props if prop not in nn_props]
        summary["null_props"] = sorted(list(set(null_props)))

        # summarize all nodes
        nn_nodes = sorted(list(set(nn_nodes)))
        summary["nn_nodes"] = nn_nodes

        dd_regex = re.compile(r"[^_][A-Za-z0-9_]+")
        dd_nodes = list(filter(dd_regex.match, list(dd)))
        dd_nodes = [node for node in dd_nodes if node not in omit_nodes]
        null_nodes = [node for node in dd_nodes if node not in nn_nodes]

        summary["null_nodes"] = null_nodes

        if write_report: # write_report == True

            self.create_output_dir(outdir=outdir)

            outname = "data_summary_{}.tsv".format(batch_tsvs["batch"])
            outname = "{}/{}".format(
                outdir, outname
            )  # ./data_summary_prod_tsvs_04272020.tsv

            report.to_csv(outname, sep="\t", index=False, encoding="utf-8")
            sys.stdout.write("\rReport written to file:".ljust(200, " "))
            print("\n\t{}".format(outname))

        return summary




#


    def create_mock_files(self,
        project_id="DEV-test",
        count=3,
        prefix="mock_data_file",
        file_format="dcm",
        outdir=".",
        msg = "This is a mock data file for testing purposes. Delete me!",
        write_tsv = True
        ):
        """
        Create some mock data file objects to use in QA / mock ups.
        """
        prog,proj = project_id.split("-")
        authz = ["/programs/{}/projects/{}".format(prog,proj)]
        acl = [prog,proj]

        mfiles = {'file_name':[],'md5sum':[],"file_size":[],"object_id":[],"storage_urls":[],"acl":[],"authz":[]}
        for i in range(count):
            file_name = "{}_{}.{}".format(prefix,i+1,file_format)
            object_id = str(uuid.uuid4())
            mfiles['file_name'].append(file_name)
            mfiles['object_id'].append(object_id)
            mfiles['authz'].append(authz)
            mfiles['acl'].append(acl)


            output = "{}/{}".format(outdir,file_name)
            os.system("touch {}".format(output))
            file_msg ="{} File {} of {}. {} with object_id {}.".format(msg,i+1,count,file_name,object_id)
            cmd = 'echo "{}" > {}'.format(file_msg,file_name)
            os.system(cmd)

            with open(output, 'rb') as file_to_check:
                file_contents = file_to_check.read()
                #cmd = "!md5 mock_data_file_{}.{}".format(i+1,file_format))
                md5 = hashlib.md5(file_contents).hexdigest() #check in shell: !md5 mock_data_file_3.dcm

            mfiles['md5sum'].append(md5)
            mfiles['file_size'].append(os.stat(output).st_size)
            urls="s3://this-is-a-fake-url-for:{}".format(file_name)
            mfiles['storage_urls'].append([urls])

        return mfiles






    def index_mock_files(self,mfiles):
        """
        Create indexd records for some fake / mock data files created by create_mock_files() func.
        Args:
            mfiles = {'file_name':[],'md5sum':[],"file_size":[],"object_id":[],"storage_urls":[],"acl":[],"authz":[]}
        """
        results = []
        for i in range(len(mfiles['file_name'])):
            print("Submitting {} to indexd at {}.".format(mfiles['file_name'][i],mfiles['object_id'][i]))
            res = self.create_record(
                    did=mfiles['object_id'][i],
                    hashes={'md5':mfiles['md5sum'][i]},
                    size=mfiles['file_size'][i],
                    urls=mfiles['storage_urls'][i],
                    file_name=mfiles['file_name'][i],
                    acl=mfiles['acl'][i],
                    authz=mfiles['authz'][i])
            results.append(res)
        return results

    def create_mock_tsv(self,
        dd,
        node,
        count,
        parent_tsvs=None,
        outdir=".",
        filename=None,
        links=None,
        project_id=None,
        excluded_props = [
            "id",
            "submitter_id",
            "type",
            "project_id",
            "created_datetime",
            "updated_datetime",
            "state",
            "file_state",
            "error_type"],
        file_props = [
            "file_name",
            "file_size",
            "md5sum",
            "object_id",
            "storage_urls"],
        mfiles=None,
        submit_tsv=False,
        minimum=1,
        maximum=20
        ):
        """
        Create mock / simulated data in a submission TSV for a node in the data dictionary.
        Args:
            dd (dict): the Gen3 data dictionary you get with Gen3Submission.get_dictionary_all()
            node(str): the name of the node in the data dictionary
            count(int): the number of records / rows to create in the submission TSV
            parent_tsvs(dict): a dictionary of node names (keys) and filenames (values) containing the parent node submission TSV; if left blank, the function will not include link submitter_ids; e.g., parent_tsvs = {'cases':'case_mock_1.1.4.tsv'}
            outdir(str): the local directory to write simulated TSV data to
            filename(str): the filename to use, default is the name of the node
            links(list): a list of links to include in the submission TSV
            excluded_props(list): a list of properties in data dictionary to ignore / exclude from the TSV columns
            mfiles(dict): a dictionary of mock data files created using func create_mock_files()
            submit_tsv(boolean): if true, will use sdk to submit the file via sheepdog

        ############################################################
        ############################################################
        # Use these settings for testing, comment out when actually running as function or in SDK.
        ############################################################
        dd = sub.get_dictionary_all()
        dd_version = dd["_settings"]["_dict_version"]
        node = 'cr_series_file'
        count = 3
        outdir = "/Users/christopher/Documents/Notes/MIDRC/annotations/sample_data/DEV-test/script_tsvs"
        filename = "{}_mock_{}.tsv".format(node,dd_version) # override for testing, comment out
        links = self.list_links(node, dd)

        parent_tsvs = {link:"{}/{}_mock_{}.tsv".format(outdir,link_targets[link],dd_version) for link in links}
        links = None # for testing comment this out later
        ############################################################
        ############################################################
        ############################################################
        """
        # get the data dictionary and version number
        dd_version = dd["_settings"]["_dict_version"]

        data = {}
        data['type'] = [node] * count
        data['submitter_id'] = ["{}-{}".format(node,i+1) for i in range(count)]

        props = list(dd[node]["properties"])
        props = list(set(props).difference(excluded_props))

        # build list of link_names to filter excluded nodes out of links
        if links is None: # if user didn't specify the links to be used, use them all.
            links = self.list_links(node, dd)
            if 'subgroup' in dd[node]['links'][0]:
                link_targets = {i['name']:i['target_type'] for i in dd[node]['links'][0]['subgroup']}
            else:
                link_targets = {i['name']:i['target_type'] for i in dd[node]['links']} #get targets to filter out excluded nodes

        link_names = {}
        for link in links:
            props.remove(link) if link in props else False # remove the links bc missing ".submitter_id", will add back below
            if link == "projects":
                link_name = "projects.code"
            else:
                target_type = link_targets[link]
                link_name = "{}.submitter_id".format(link)
            link_names[link] = link_name

        # add links to data
        for link in link_names:
            link_name = link_names[link]
            if link_name == 'projects.code' and project_id is not None:
                prog,proj = project_id.split("-",1)
                data[link_name] = [proj] * count
            elif parent_tsvs is None: #
                data[link_name] = [np.nan] * count
            else:
                parent_tsv = parent_tsvs[link]
                pdf = pd.read_csv(parent_tsv,sep='\t',header=0)
                psids = list(set(pdf['submitter_id']))
                available_psids = cycle(psids)
                data[link_name] = [next(available_psids)for i in range(count)]

        if mfiles is not None:
            props = list(set(props).difference(file_props))
            if len(mfiles['file_name']) != count:
                print("The number of mock data files provided in 'mfiles' ({}) does not match the 'count' provided ({})!".format(len(mfiles['file_name']),count))
            for file_prop in file_props:
                if file_prop in mfiles:
                    data[file_prop] = mfiles[file_prop]
                else:
                    print("File property '{}' is missing from mfiles! \n\t{}".format(file_prop,list(mfiles)))

        for prop in props:
            if prop == 'file_name':
                data['file_name'] = [sid + ".mock_filename.txt" for sid in data['submitter_id']]
            elif prop == 'md5sum':
                md5s = []
                for i in range(count):
                    md5 = str(hashlib.md5(b"test").hexdigest())
                    md5s.append(md5)
                data['md5sum'] = md5s
            elif prop == 'object_id':
                # add blank column to fill later upon submission (will create indexd records to get object_ids)
                data['object_id'] = [np.nan] * count
                # object_ids = []
                # for i in range(count):
                    # irec = index.create_blank(uploader="cgmeyer@uchicago.edu",file_name="thisisatest.filename")
                    # object_ids.append(irec['did'])
                # OR
                #     object_ids.append(str(uuid.uuid4())) # guids will need to be created in indexd later for sheepdog submission to work
                # data['object_id'] = object_ids
            elif 'type' in dd[node]['properties'][prop]:
                prop_type = dd[node]['properties'][prop]['type'] # expected type
                if prop_type == 'array':
                    if 'items' in dd[node]['properties'][prop]:
                        array_type = dd[node]['properties'][prop]['items']
                        if 'type' in dd[node]['properties'][prop]['items']:
                            array_type = dd[node]['properties'][prop]['items']['type']
                        if 'minimum' in dd[node]['properties'][prop]['items']:
                            minimum = dd[node]['properties'][prop]['items']['minimum']
                        if 'maximum' in dd[node]['properties'][prop]['items']:
                            maximum = dd[node]['properties'][prop]['items']['maximum']
                    if array_type == "string":
                        data[prop] = ["test {}, test {}".format(prop,prop)] * count
                        # array_values = ["test {}, test {}".format(prop,prop)] * count
                        # data[prop] = ','.join(array_values)
                    elif array_type == "integer":
                        array_list = []
                        for i in range(count):
                            array_list.append(",".join(map(str,list(np.random.randint(low=1, high=89, size=(2))))))
                        data[prop] = array_list
                    elif array_type == "number":
                        array_list = []
                        for i in range(count):
                            one_array = list(np.random.uniform(low=1, high=89, size=(2)))
                            formatted_array = [ '%.2f' % elem for elem in one_array ]
                            array_list.append(",".join(map(str,formatted_array)))
                        data[prop] = array_list
                    elif array_type == "enum":
                        print("do something")
                elif prop_type == "string":
                    data[prop] = ["test " + prop] * count
                elif prop_type == "boolean":
                    available_types = cycle([True,False])
                    data[prop] = [next(available_types)for i in range(count)]
                elif prop_type == "integer":
                    if 'minimum' in dd[node]['properties'][prop]:
                        minimum = dd[node]['properties'][prop]['minimum']
                    if 'maximum' in dd[node]['properties'][prop]:
                        maximum = dd[node]['properties'][prop]['maximum']
                    data[prop] = list(np.random.randint(low=minimum, high=maximum, size=(count)))
                elif prop_type == "number":
                    if 'minimum' in dd[node]['properties'][prop]:
                        minimum = dd[node]['properties'][prop]['minimum']
                    if 'maximum' in dd[node]['properties'][prop]:
                        maximum = dd[node]['properties'][prop]['maximum']
                    data[prop] = [ '%.2f' % elem for elem in list(np.random.uniform(low=minimum, high=maximum, size=count))]

            elif 'enum' in dd[node]['properties'][prop]:
                enums = dd[node]['properties'][prop]['enum']
                available_enums = cycle(enums)
                #enum_values = ['a','b']
                #available_enums = cycle(enum_values)
                data[prop] = [next(available_enums)for i in range(count)]

        # create a dataframe and save as a TSV
        df = pd.DataFrame(data)

        # save dataframe to TSV file
        if filename is None:
            filename = "{}_mock_{}.tsv".format(node,dd_version)

        Path(outdir).mkdir(parents=True, exist_ok=True)

        output = "{}/{}".format(outdir,filename)
        df.to_csv(output,sep='\t',index=False)

        if submit_tsv is True:
            filename = "{}_mock_{}.tsv".format(node,dd_version)
            output = "{}/{}".format(outdir,filename)
            self.submit_file(project_id="DEV-test",filename=output)
        return df

    def create_mock_project(self,
        dd,
        node_counts=None,
        project_id=None,
        outdir="mock_tsvs",
        excluded_props = [
            "id",
            "submitter_id",
            "type",
            "project_id",
            "created_datetime",
            "updated_datetime",
            "state",
            "file_state",
            "error_type"],
        file_props = [
            "file_name",
            "file_size",
            "md5sum",
            "object_id",
            "storage_urls"],
        excluded_nodes=[],
        submit_tsvs=False
        ):
        """

        Create mock / simulated data project for a list of nodes in the data dictionary. Ignores program/project root nodes, so make sure those exist first. This is a wrapper for the func Gen3Expansion.create_mock_tsv()
        Args:
            dd (dict): the Gen3 data dictionary you get with Gen3Submission.get_dictionary_all().
            node_counts(dict): node_ids as keys, values is number of records to create for that node.
                For example: {"case":3,"imaging_study":6}
            project_id(str): If no project_id is provided, using the generic 'DEV-test' project_id
            outdir(str): the local directory to write simulated TSV data to.
            excluded_props(list): a list of properties in data dictionary to ignore / exclude from TSVs.
            file_props(list): a list of file_properties to be simulated; unlikely to change from default.
            excluded_nodes(list): a list of nodes to not create mock TSVs for.
            submit_tsvs(boolean): if true, will use sdk to submit the DataFrames via sheepdog
        """
        dd_version = dd["_settings"]["_dict_version"]
        if project_id is None:
            print("\tNo 'project_id' provided; using the generic 'DEV-test' as the project_id.")
            project_id = "DEV-test"
        prog,proj = project_id.split("-",1)

        # for the create_mock_tsv() func, we need "node", "count" and "parent_tsvs".

        # Build node_counts if not provided; this gets us "node" and "count"
        node_counts=None
        if node_counts is None:
            node_order = self.get_submission_order()
            node_counts = {}
            for node in node_order:
                node_id = node[0]
                node_count = node[1]
                print(node_id)
                if node_id == "project" or node_id in excluded_nodes: # skip project node
                    continue
                else:
                    node_counts[node_id] = node_count*node_count # get progressively larger counts as you go down in data model hierarchy
            print("\tNo node_counts provided; using the following node_counts:\n\t{}".format(node_counts))

        # Now build "parent_tsvs" for each node in "node_counts":
        all_parent_tsvs = {}
        for node in node_counts:
            print(node)
            parent_tsvs = {}
            node_links = dd[node]['links'][0]
            if 'subgroup' in node_links:
                sublinks = node_links['subgroup']
                link_targets = {i['name']:i['target_type'] for i in sublinks if i['target_type'] not in excluded_nodes}
                if node_links['exclusive'] == True: # check if subgroup links are exclusive
                    random_link = random.choice(list(link_targets.items())) # pick only one random link if exclusive
                    link_targets = {random_link[0]:random_link[1]}
            else:
                link_targets = {i['name']:i['target_type'] for i in dd[node]['links'] if i['target_type'] not in excluded_nodes} #get targets to filter out excluded nodes

            for link in link_targets:
                parent_tsvs[link] = "{}/{}_mock_{}.tsv".format(outdir,link_targets[link],dd_version)
            #print("\t\t{}".format(parent_tsvs))
            all_parent_tsvs[node] = parent_tsvs

        # Create the TSVs
        for node in node_counts:
            # Create the node TSV / DataFrame
            df = self.create_mock_tsv(
                dd=dd,
                node=node,
                count=node_counts[node],
                parent_tsvs=all_parent_tsvs[node],
                project_id=project_id,
                outdir=outdir,
            )
            if submit_tsvs:
                if 'object_id' in df and df['object_id'].isnull().values.any():
                    object_ids = []
                    for i in range(len(df)):
                        file_name = list(df['file_name'])[i]
                        size = list(df['file_size'])[i]
                        md5 = list(df['md5sum'])[i]
                        try:
                            irec = self.create_mock_indexd_record(
                                file_name=file_name,
                                md5=md5,
                                size=size,
                                project_id=project_id)
                        except:
                            print("Couldn't create the indexd record for file {}:\n\t{}".format(file_name,irec))
                        object_id = irec['did']
                        object_ids.append(object_id)
                    df['object_id'] = object_ids
                d = self.submit_df(project_id=project_id, df=df, chunk_size=250)


    def create_mock_indexd_record(self,
        file_name,
        md5,
        size,
        project_id="DEV-test",
        uploader="cgmeyer@uchicago.edu"):
        """
        Create a blank indexd record}
        """
        prog,proj = project_id.split("-",1)
        iurl = "{}index/index".format(self._endpoint)
        payload = {'form': 'object',
            'file_name':file_name,
            'hashes':{'md5':md5},
            'size':size,
            'authz':["/programs/{}/projects/{}".format(prog,proj)],
            'acl':[prog,proj],
            'urls':['s3://mock/bucket/{}'.format(file_name)],
            #'uploader':uploader
        }
        try:
            res = requests.post(
                iurl,
                headers={"content-type": "application/json"},
                auth=self._auth_provider,
                data=json.dumps(payload),
            )
        except:
            print("\n\tError creating indexd record:\n{}\n{}\n".format(res,res.text))
        data = res.json()
        return data

    def create_blank_indexd_record(self, uploader="cgmeyer@uchicago.edu", file_name=None):
        """
        Create a blank indexd record}
        """
        iurl = "{}index/index/blank".format(self._endpoint)
        payload = {"uploader": uploader, "file_name": file_name}
        res = requests.post(
            iurl,
            headers={"content-type": "application/json"},
            auth=self._auth_provider,
            data=json.dumps(payload),
        )
        try:
            data = res.json()
            return data
        except:
            print("\n\tNo json in indexd response:\n{}\n{}\n".format(res,res.text))
            return res.text




    def nuked(self,message="Deleted!"):
        mushroom_cloud1 = """
                 _.-^^---....,,--
             _--                  --_
            <                        >)
        """
        mushroom_cloud2 = """
            |                         |
             \._                   _./
                ```--. . , ; .--'''
                      | |   |
                   .-=||  | |=-.
                   `-=#$%&%$#=-'
                      | ;  :|
             _____.,-#%&$@%#&#~,._____
        """
        print(mushroom_cloud1)
        print("\t\t{}".format(message))
        print(mushroom_cloud2)

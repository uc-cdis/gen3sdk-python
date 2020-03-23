import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re, operator
import pandas as pd
from os import path
from pandas.io.json import json_normalize
from collections import Counter
from statistics import mean

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

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

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint
        self.sub = Gen3Submission(endpoint, auth_provider)

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
        print("Output written to file: "+filename+"\n")

    ### AWS S3 Tools:
    def s3_ls(self, path, bucket, profile, pattern='*'):
        ''' Print the results of an `aws s3 ls` command '''
        s3_path = bucket + path
        cmd = ['aws', 's3', 'ls', s3_path, '--profile', profile]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
        except Exception as e:
            output = e.output.decode('UTF-8')
            print("ERROR:" + output)
        psearch = output.split('\n')
        if pattern != '*':
            pmatch = fnmatch.filter(psearch, pattern) #if default '*', all files will match
            return arrayTable(pmatch)
        else:
            return output

    def s3_files(self, path, bucket, profile, pattern='*',verbose=True):
        ''' Get a list of files returned by an `aws s3 ls` command '''
        s3_path = bucket + path
        cmd = ['aws', 's3', 'ls', s3_path, '--profile', profile]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
        except Exception as e:
            output = e.output.decode('UTF-8')
            print("ERROR:" + output)
        output = [line.split() for line in output.split('\n')]
        output = [line for line in output if len(line) == 4] #filter output for lines with file info
        output = [line[3] for line in output] #grab the filename only
        output = fnmatch.filter(output, pattern) #if default '*', all files will match
        if verbose is True:
            print('\nIndex \t Filename')
            for (i, item) in enumerate(output, start=0): print(i, '\t', item)
        return output

    def get_s3_files(self, path, bucket, profile, files=None, mydir=None):
        ''' Transfer data from object storage to the VM in the private subnet '''

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
                  print("File "+filename+" already downloaded in that location.")
              else:
                  print(s3_filepath)
                  cmd = ['aws', 's3', '--profile', profile, 'cp', s3_filepath, mydir]
                  try:
                      output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
                  except Exception as e:
                      output = e.output.decode('UTF-8')
                      print("ERROR:" + output)
        # If files is None, which syncs the s3_path 'directory'
        else:
           print("Syncing directory " + s3_path)
           cmd = ['aws', 's3', '--profile', profile, 'sync', s3_path, mydir]
           try:
              output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
           except Exception as e:
              output = e.output.decode('UTF-8')
              print("ERROR:" + output)
        print("Finished")

    # Functions for downloading metadata in TSVs

    def get_project_ids(self, node=None,name=None):
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
        #Return all project_ids in the data commons if no node is provided or if node is program but no name provided
        if name is None and ((node is None) or (node is 'program')):
            print("Getting all project_ids you have access to in the data commons.")
            if node == 'program':
                print('Specify a list of program names (name = [\'myprogram1\',\'myprogram2\']) to get only project_ids in particular programs.')
            queries.append("""{project (first:0){project_id}}""")
        elif name is not None and node == 'program':
            if isinstance(name,list):
                print('Getting all project_ids in the programs \''+",".join(name)+'\'')
                for program_name in name:
                    queries.append("""{project (first:0, with_path_to:{type:"program",name:"%s"}){project_id}}""" % (program_name))
            elif isinstance(name,str):
                print('Getting all project_ids in the program \''+name+'\'')
                queries.append("""{project (first:0, with_path_to:{type:"program",name:"%s"}){project_id}}""" % (name))
        elif isinstance(node,str) and isinstance(name,str):
            print('Getting all project_ids for projects with a path to record \''+name+'\' in node \''+node+'\'')
            queries.append("""{project (first:0, with_path_to:{type:"%s",submitter_id:"%s"}){project_id}}""" % (node,name))
        elif isinstance(node,str) and name is None:
            print('Getting all project_ids for projects with at least one record in the node \''+node+'\'')
            query = """{node (first:0,of_type:"%s"){project_id}}""" % (node)
            df = json_normalize(self.sub.query(query)['data']['node'])
            project_ids = project_ids + list(set(df['project_id']))
        if len(queries) > 0:
            for query in queries:
                res = self.sub.query(query)
                df = json_normalize(res['data']['project'])
                project_ids = project_ids + list(set(df['project_id']))
        my_ids = sorted(project_ids,key=str.lower)
        print(my_ids)
        return my_ids


    def get_node_tsvs(self, node, projects=None, overwrite=False, remove_empty=True, outdir='node_tsvs'):
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
        mydir = "{}/{}_tsvs".format(outdir,node)
        if not os.path.exists(mydir):
            os.makedirs(mydir)

        if projects is None: #if no projects specified, get node for all projects
            projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        elif isinstance(projects, str):
            projects = [projects]

        dfs = []
        df_len = 0
        for project in projects:
            filename = str(mydir+'/'+project+'_'+node+'.tsv')
            if (os.path.isfile(filename)) and (overwrite is False):
                print("File previously downloaded.")
            else:
                prog,proj = project.split('-',1)
                self.sub.export_node(prog,proj,node,'tsv',filename)
            df1 = pd.read_csv(filename, sep='\t', header=0, index_col=False)
            dfs.append(df1)
            df_len+=len(df1)
            print(filename +' has '+str(len(df1))+' records.')

            if remove_empty is True:
                if df1.empty:
                    print('Removing empty file: ' + filename)
                    cmd = ['rm',filename] #look in the download directory
                    try:
                        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                    except Exception as e:
                        output = e.output.decode('UTF-8')
                        print("ERROR deleting file: " + output)

        all_data = pd.concat(dfs, ignore_index=True)
        print('length of all dfs: ' +str(df_len))
        nodefile = str('master_'+node+'.tsv')
        all_data.to_csv(str(mydir+'/'+nodefile),sep='\t',index=False)
        print('Master node TSV with '+str(len(all_data))+' total records written to '+nodefile+'.')
        return all_data

    def get_project_tsvs(self, projects=None, nodes=None, outdir='project_tsvs', overwrite=False, save_empty=False,remove_nodes = ['program','project','root','data_release']):
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
            nodes = sorted(list(set(json_normalize(self.sub.query("""{_node_type (first:-1) {id}}""")['data']['_node_type'])['id'])))  #get all the 'node_id's in the data model
        elif isinstance(nodes,str):
            nodes = [nodes]

        for node in remove_nodes:
            if node in nodes: nodes.remove(node)

        if projects is None: #if no projects specified, get node for all projects
            projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        elif isinstance(projects, str):
            projects = [projects]

        for project_id in projects:
            mydir = "{}/{}_tsvs".format(outdir,project_id) #create the directory to store TSVs

            if not os.path.exists(mydir):
                os.makedirs(mydir)

            for node in nodes:
                filename = str(mydir+'/'+project_id+'_'+node+'.tsv')
                if (os.path.isfile(filename)) and (overwrite is False):
                    print("\tPreviously downloaded: '{}'".format(filename))
                else:
                    query_txt = """{_%s_count (project_id:"%s")}""" % (node,project_id)
                    res = self.sub.query(query_txt) #  {'data': {'_acknowledgement_count': 0}}
                    count = res['data'][str('_'+node+'_count')] # count=int(0)
                    if count > 0 or save_empty is True:
                        print("\nDownloading {} records in node '{}' of project '{}'.".format(count,node,project_id))
                        prog,proj = project_id.split('-',1)
                        self.sub.export_node(prog,proj,node,'tsv',filename)
                    else:
                        print("\t{} records in node '{}' of project '{}'.".format(count,node,project_id))

        cmd = ['ls',mydir] #look in the download directory
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
        except Exception as e:
            output = 'ERROR:' + e.output.decode('UTF-8')

        return output

# Query Functions
    def paginate_query_old(self, node, project_id=None, props=['id','submitter_id'], chunk_size=10000, format='json',args=None):
        """Function to paginate a query to avoid time-outs.
        Returns a json of all the records in the node.

        Args:
            node (str): The node to query.
            project_id(str): The project_id to limit the query to. Default is None.
            props(list): A list of properties in the node to return.
            chunk_size(int): The number of records to return per query. Default is 10000.
            args(str): Put graphQL arguments here. For example, 'with_path_to:{type:"case",submitter_id:"case-01"}', etc. Don't enclose in parentheses.
        Example:
            paginate_query('demographic')
        """

        if node == 'datanode':
            query_txt = """{ %s (%s) { type } }""" % (node, args)
            response = self.sub.query(query_txt)
            if 'data' in response:
                nodes = [record['type'] for record in response['data']['datanode']]
                if len(nodes) > 1:
                    print("\tMultiple files with that file_name exist across multiple nodes:\n\t{}.".format(nodes))
                elif len(nodes) == 1:
                    node = nodes[0]
                else:
                    return nodes

        if project_id is not None:
            program,project = project_id.split('-',1)
            if args is None:
                query_txt = """{_%s_count (project_id:"%s")}""" % (node, project_id)
            else:
                query_txt = """{_%s_count (project_id:"%s", %s)}""" % (node, project_id, args)
        else:
            if args is None:
                query_txt = """{_%s_count}""" % (node)
            else:
                query_txt = """{_%s_count (%s)}""" % (node, args)


        # First query the node count to get the expected number of results for the requested query:

        try:
            res = self.sub.query(query_txt)
            count_name = '_'.join(map(str,['',node,'count']))
            qsize = res['data'][count_name]
            print("\tFound {} records in '{}' node of project '{}'. ".format(qsize,node,project_id))
        except:
            print("Query to get _{}_count failed! {}".format(node,query_txt))

        #Now paginate the actual query:
        properties = ' '.join(map(str,props))
        offset = 0
        total = {}
        total['data'] = {}
        total['data'][node] = []
        count = 0
        while offset < qsize:

            if project_id is not None:
                if args is None:
                    query_txt = """{%s (first: %s, offset: %s, project_id:"%s"){%s}}""" % (node, chunk_size, offset, project_id, properties)
                else:
                    query_txt = """{%s (first: %s, offset: %s, project_id:"%s", %s){%s}}""" % (node, chunk_size, offset, project_id, args, properties)
            else:
                if args is None:
                    query_txt = """{%s (first: %s, offset: %s){%s}}""" % (node, chunk_size, offset, properties)
                else:
                    query_txt = """{%s (first: %s, offset: %s, %s){%s}}""" % (node, chunk_size, offset, args, properties)

            res = self.sub.query(query_txt)
            if 'data' in res:
                records = res['data'][node]

                if len(records) < chunk_size:
                    if qsize == 999999999:
                        return total

                total['data'][node] +=  records # res['data'][node] should be a list
                offset += chunk_size
            elif 'error' in res:
                print(res['error'])
                if chunk_size > 1:
                    chunk_size = int(chunk_size/2)
                    print("Halving chunk_size to: "+str(chunk_size)+".")
                else:
                    print("Query timing out with chunk_size of 1!")
                    exit(1)
            else:
                print("Query Error: "+str(res))

            print("\tRecords retrieved: "+str(len(total['data'][node]))+" of "+str(qsize)+" (Query offset: "+str(offset)+", Query chunk_Size: "+str(chunk_size)+").")

        if format is 'tsv':
            df = json_normalize(total['data'][node])
            return df
        else:
            return total

    def paginate_query(self, node, project_id=None, props=[], args=None, chunk_size=5000, offset=0, format='json'):
        """Function to paginate a query to avoid time-outs.
        Returns a json of all the records in the node.

        Args:
            node (str): The node to query.
            project_id(str): The project_id to limit the query to. Default is None.
            props(list): A list of properties in the node to return.
            chunk_size(int): The number of records to return per query. Default is 10000.
            args(str): Put graphQL arguments here. For example, 'with_path_to:{type:"case",submitter_id:"case-01"}', etc. Don't enclose in parentheses.
        Example:
            paginate_query('demographic')
        """
        props = list(set(['id','submitter_id']+props))
        properties = ' '.join(map(str,props))

        if project_id is not None:
            if args is None:
                query_txt = """{%s (first: %s, offset: %s, project_id:"%s"){%s}}""" % (node, chunk_size, offset, project_id, properties)
            else:
                query_txt = """{%s (first: %s, offset: %s, project_id:"%s", %s){%s}}""" % (node, chunk_size, offset, project_id, args, properties)
        else:
            if args is None:
                query_txt = """{%s (first: %s, offset: %s){%s}}""" % (node, chunk_size, offset, properties)
            else:
                query_txt = """{%s (first: %s, offset: %s, %s){%s}}""" % (node, chunk_size, offset, args, properties)

        total = {}
        total['data'] = {}
        total['data'][node] = []

        records = list(range(chunk_size))
        while len(records) == chunk_size:

            res = self.sub.query(query_txt)

            if 'data' in res:
                records = res['data'][node]
                total['data'][node] +=  records # res['data'][node] should be a list
                offset += chunk_size

            elif 'error' in res:
                print(res['error'])
                if chunk_size > 1:
                    chunk_size = int(chunk_size/2)
                    print("\tHalving chunk_size to: {}.".format(chunk_size))
                else:
                    print("\tQuery timing out with chunk_size of 1!")
                    exit(1)

            else:
                print("Query Error: {}".format(res))

            print("\tTotal records retrieved: {}".format(len(total['data'][node])))

        if format is 'tsv':
            df = json_normalize(total['data'][node])
            return df
        else:
            return total


    def get_uuids_in_node(self,node,project_id):
        """
        This function returns a list of all the UUIDs of records
        in a particular node of a given project.
        """
        program,project = project_id.split('-',1)

        try:
            res = self.paginate_query(node,project_id)
            uuids = [x['id'] for x in res['data'][node]]
        except:
            raise Gen3Error("Failed to get UUIDs in node '"+node+"' of project '"+project_id+"'.")

        return uuids

    def list_project_files(self, project_id):
        query_txt = """{datanode(first:-1,project_id: "%s") {type file_name id object_id}}""" % (project_id)
        res = self.sub.query(query_txt)
        if len(res['data']['datanode']) == 0:
            print('Project ' + project_id + ' has no records in any data_file node.')
            return None
        else:
            df = json_normalize(res['data']['datanode'])
            json_normalize(Counter(df['type']))
            #guids = df.loc[(df['type'] == node)]['object_id']
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
            res = self.paginate_query(node=node,args=args)
            recs = res['data'][node]
            if len(recs) == 1:
                uuids.append(recs[0]['id'])
            elif len(recs) == 0:
                print("No data returned for {}:\n\t{}".format(sid,res))
            print("\t{}/{}".format(count,len(sids)))
        print("Finished retrieving {} uuids for {} submitter_ids".format(len(uuids),len(sids)))
        return uuids

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
        program,project = project_id.split('-',1)

        if isinstance(uuids, str):
            uuids = [uuids]

        if not isinstance(uuids, list):
            raise Gen3Error("Please provide a list of UUID(s) to delete with the 'uuid' argument.")

        if backup:
            ext = backup.split('.')[-1]
            fname = ".".join(backup.split('.')[0:-1])
            count = 0
            while path.exists(backup):
                count+=1
                backup = "{}_{}.{}".format(fname,count,ext)

            count = 0
            print("Attempting to backup {} records to delete to file '{}'.".format(len(uuids),backup))

            records = []
            for uuid in uuids:
                count+=1
                try:
                    response = self.sub.export_record(program=program,project=project,uuid=uuid,fileformat='json',filename=None)
                    record = json.loads(json.dumps(response[0]))
                    records.append(record)
                    print("\tRetrieving record for UUID '{}' ({}/{}).".format(uuid,count,len(uuids)))
                except Exception as e:
                    print("Exception occurred during 'export_record' request: {}.".format(e))
                    continue

            with open(backup,'w') as backfile:
                backfile.write("{}".format(records))

        responses = []
        errors = []
        failure = []
        success = []
        retry = []
        tried = []
        results = {}

        while len(tried) < len(uuids): #loop sorts all uuids into success or failure

            if len(retry) > 0:
                print("Retrying deletion of {} valid UUIDs.".format(len(retry)))
                list_ids = ",".join(retry)
                retry = []
            else:
                list_ids  = ",".join(uuids[len(tried):len(tried)+chunk_size])

            rurl = "{}/api/v0/submission/{}/{}/entities/{}".format(
                self._endpoint,program,project,list_ids
            )

            try:
                #print("\n\trurl='{}'\n".format(rurl)) # trouble-shooting
                #print("\n\tresp = requests.delete(rurl, auth=auth)")
                #print("\n\tprint(resp.text)")
                resp = requests.delete(rurl, auth=self._auth_provider)
            except Exception as e:
                chunk_size = int(chunk_size/2)
                print("Exception occurred during delete request:\n\t{}.\n\tReducing chunk_size to '{}'.".format(e,chunk_size))
                continue

            if "414 Request-URI Too Large" in resp.text or "service failure" in resp.text:
                chunk_size = int(chunk_size/2)
                print("Service Failure. The chunk_size is too large. Reducing to '{}'".format(chunk_size))
            elif "The requested URL was not found on the server." in resp.text:
                print("\n Requested URL not found on server:\n\t{}\n\t{}".format(resp,rurl)) #debug
                break
            else: # the delete request got an API response
                #print(resp.text) #trouble-shooting
                output = json.loads(resp.text)
                responses.append(output)

                if output['success']: # 'success' is True or False in API response
                    success = list(set(success + [x['id'] for x in output['entities']]))
                else: # if one UUID fails to delete in the request, the entire request fails.
                    for entity in output['entities']:
                        if entity['valid']: # get the valid entities from repsonse to retry.
                            retry.append(entity['id'])
                        else:
                            errors.append(entity['errors'][0]['message'])
                            failure = list(set(failure.append(entity['id'])))
                    for error in list(set(errors)):
                        print("Error message for {} records: {}".format(errors.count(error),error))

            tried = list(set(success + failure))
            print("\tProgress: {}/{} (Success: {}, Failure: {}).".format(len(tried),len(uuids),len(success),len(failure)))

        # exit the while loop if
        results['failure'] = failure
        results['success'] = success
        results['responses'] = responses
        results['errors'] = errors
        print("\tFinished record deletion script.")

        return results


    def delete_node(self,node,project_id,chunk_size=200):
        """
        This function attempts to delete all the records in a particular node of a project.
        It returns the results of the delete_records function.
        """
        try:
            uuids = self.get_uuids_in_node(node,project_id)
        except:
            raise Gen3Error("Failed to get UUIDs in the node '"+node+"' of project '"+project_id+"'.")

        if len(uuids) != 0:
            print("Attemping to delete "+str(len(uuids))+" records in the node '"+node+"' of project '"+project_id+"'.")

            try:
                results = self.delete_records(uuids, project_id, chunk_size)
                print("Successfully deleted "+str(len(results['success']))+" records in the node '"+node+"' of project '"+project_id+"'.")
                if len(results['failure']) > 0:
                    print("Failed to delete "+str(len(results['failure']))+" records. See results['errors'] for the error messages.")
            except:
                raise Gen3Error("Failed to delete UUIDs in the node '"+node+"' of project '"+project_id+"'.")

            return results

    def get_submission_order(self,root_node='project',excluded_schemas=['_definitions','_settings','_terms','program','project','root','data_release','metaschema']):
        """
        This function gets a data dictionary, and then it determines the submission order of nodes by looking at the links.
        The reverse of this is the deletion order for deleting projects. (Must delete child nodes before parents).
        """
        dd = self.sub.get_dictionary_all()
        schemas = list(dd)
        nodes = [k for k in schemas if k not in excluded_schemas]
        submission_order = [(root_node,0)] # make a list of tuples with (node, order) where order is int
        while len(submission_order) < len(nodes)+1: # "root_node" is not in "nodes", thus the +1
            for node in nodes:
                if len([item for item in submission_order if node in item]) == 0: #if the node is not in submission_order
                    #print("Node: {}".format(node))
                    node_links = dd[node]['links']
                    parents = []
                    for link in node_links:
                        if 'target_type' in link: #node = 'webster_step_second_test'
                            parents.append(link['target_type'])
                        elif 'subgroup' in link: # node = 'expression_array_result'
                            sub_links = link.get('subgroup')
                            if not isinstance(sub_links, list):
                                sub_links = [sub_links]
                            for sub_link in sub_links:
                                if 'target_type' in sub_link:
                                    parents.append(sub_link['target_type'])
                    if False in [i in [i[0] for i in submission_order] for i in parents]:
                        continue # if any parent is not already in submission_order, skip this node for now
                    else: # submit this node after the last parent to submit
                        parents_order = [item for item in submission_order if item[0] in parents]
                        submission_order.append((node,max([item[1] for item in parents_order]) + 1))
        return submission_order

    def delete_project(self,project_id,root_node='project',chunk_size=200):
        submission_order = self.get_submission_order(root_node=root_node)
        delete_order = sorted(submission_order, key=lambda x: x[1], reverse=True)
        nodes = [i[0] for i in delete_order]
        try:
            nodes.remove('project')
        except:
            print("No 'project' node in list of nodes.")
        for node in nodes:
            print("\nDeleting node '{}' from project '{}'.".format(node,project_id))
            data = self.delete_node(node=node,project_id=project_id,chunk_size=chunk_size)
        prog,proj = project_id.split('-',1)
        try:
            data = self.sub.delete_project(program=prog,project=proj)
        except Exception as e:
            print("Couldn't delete project '{}':\n\t{}".format(project_id,e))
        if "Can not delete the project." in data:
            print("{}".format(data))
        else:
            print("Successfully deleted the project '{}'".format(project_id))

# Analysis Functions
    def property_counts_table(self, prop, df):
        df = df[df[prop].notnull()]
        counts = Counter(df[prop])
        df1 = pd.DataFrame.from_dict(counts, orient='index').reset_index()
        df1 = df1.rename(columns={'index':prop, 0:'count'}).sort_values(by='count', ascending=False)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            display(df1)

    def property_counts_by_project(self, prop, df):
        df = df[df[prop].notnull()]
        categories = list(set(df[prop]))
        projects = list(set(df['project_id']))

        project_table = pd.DataFrame(columns=['Project','Total']+categories)
        project_table

        proj_counts = {}
        for project in projects:
            cat_counts = {}
            cat_counts['Project'] = project
            df1 = df.loc[df['project_id']==project]
            total = 0
            for category in categories:
                cat_count = len(df1.loc[df1[prop]==category])
                total+=cat_count
                cat_counts[category] = cat_count

            cat_counts['Total'] = total
            index = len(project_table)
            for key in list(cat_counts.keys()):
                project_table.loc[index,key] = cat_counts[key]

            project_table = project_table.sort_values(by='Total', ascending=False, na_position='first')

        return project_table

    def plot_categorical_property(self, property, df):
        #plot a bar graph of categorical variable counts in a dataframe
        df = df[df[property].notnull()]
        N = len(df)
        categories, counts = zip(*Counter(df[property]).items())
        y_pos = np.arange(len(categories))
        plt.bar(y_pos, counts, align='center', alpha=0.5)
        #plt.figtext(.8, .8, 'N = '+str(N))
        plt.xticks(y_pos, categories)
        plt.ylabel('Counts')
        plt.title(str('Counts by '+property+' (N = '+str(N)+')'))
        plt.xticks(rotation=90, horizontalalignment='center')
        #add N for each bar
        plt.show()

    def plot_numeric_property(self, property, df, by_project=False):
        #plot a histogram of numeric variable in a dataframe
        df = df[df[property].notnull()]
        data = list(df[property]).astype(float)
        N = len(data)
        fig = sns.distplot(data, hist=False, kde=True,
                 bins=int(180/5), color = 'darkblue',
                 kde_kws={'linewidth': 2})
#        plt.figtext(.8, .8, 'N = '+str(N))
        plt.xlabel(property)
        plt.ylabel("Probability")
        plt.title("PDF for all projects "+property+' (N = '+str(N)+')') # You can comment this line out if you don't need title
        plt.show(fig)

        if by_project is True:
            projects = list(set(df['project_id']))
            for project in projects:
                proj_df = df[df['project_id']==project]
                data = list(proj_df[property])
                N = len(data)
                fig = sns.distplot(data, hist=False, kde=True,
                         bins=int(180/5), color = 'darkblue',
                         kde_kws={'linewidth': 2})
#                plt.figtext(.8, .8, 'N = '+str(N))
                plt.xlabel(property)
                plt.ylabel("Probability")
                plt.title("PDF for "+property+' in ' + project+' (N = '+str(N)+')') # You can comment this line out if you don't need title
                plt.show(fig)

    def plot_numeric_property_by_category(self, numeric_property, category_property, df):
        #plot a histogram of numeric variable in a dataframe
        df = df[df[numeric_property].notnull()]
        data = list(df[numeric_property])
        N = len(data)

        categories = list(set(df[category_property]))
        for category in categories:
            df_2 = df[df[category_property]==category]
            if len(df_2) != 0:
                data = list(df_2[numeric_property].astype(float))
                N = len(data)
                fig = sns.distplot(data, hist=False, kde=True,
                         bins=int(180/5), color = 'darkblue',
                         kde_kws={'linewidth': 2})
    #            plt.figtext(.8, .8, 'N = '+str(N))
                plt.xlabel(numeric_property)
                plt.ylabel("Probability")
                plt.title("PDF of "+numeric_property+' for ' + category +' (N = '+str(N)+')') # You can comment this line out if you don't need title
                plt.show(fig)

    def plot_numeric_by_category(self, numeric_property, category_property, df):
        sns.set(style="darkgrid")
        categories = list(set(df[category_property]))

        N = 0
        for category in categories:
            subset = df[df[category_property] == category]
            N += len(subset)
            data = subset[numeric_property].dropna().astype(float)
            fig = sns.distplot(data, hist = False, kde = True,
                         bins = 3, kde_kws = {'linewidth': 2}, label = category)

            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

        plt.title(numeric_property+' by ' + category_property +' (N = '+str(N)+')') # You can comment this line out if you don't need title
        plt.show(fig)

    def plot_top10_numeric_by_category(self, numeric_property, category_property, df):
        sns.set(style="darkgrid")
        categories = list(set(df[category_property]))

        category_means = {}
        for category in categories:
            df_2 = df[df[numeric_property].notnull()]
            data = list(df_2.loc[df_2[category_property]==category][numeric_property])

            if len(data) > 5:
                category_means[category] = mean(data)

        if len(category_means) > 1:
            sorted_means = sorted(category_means.items(), key=operator.itemgetter(1), reverse=True)[0:10]
            categories_list = [x[0] for x in sorted_means]

        N = 0
        for category in categories_list:
            subset = df[df[category_property] == category]
            N += len(subset)
            data = subset[numeric_property].dropna().astype(float)
            fig = sns.distplot(data, hist = False, kde = True,
                         bins = 3, kde_kws = {'linewidth': 2}, label = category)

            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

        plt.title(numeric_property+' by ' + category_property +' (N = '+str(N)+')')
        plt.show(fig)

    def plot_numeric_property_by_2_categories(self, numeric_property, category_property, category_property_2, df):

        df = df[df[numeric_property].notnull()]
        data = list(df[numeric_property])
        N = len(data)
        categories = list(set(df[category_property]))

        for category in categories:
            df_2 = df[df[category_property]==category]
            categories_2 = list(set(df_2[category_property_2])) #This is a list of all compounds tested for each tissue type.

            N = 0
            for category_2 in categories_2:
                subset = df_2[df_2[category_property_2] == category_2]
                N += len(subset)
                data = subset[numeric_property].dropna().astype(float)
                fig = sns.distplot(data, hist = False, kde = True,
                            bins = 3,
                            kde_kws = {'linewidth': 2}, label = category_2)

                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

            plt.title(numeric_property+' for ' + category +' (N = '+str(N)+')') # You can comment this line out if you don't need title
            plt.show(fig)



    def plot_top10_numeric_property_by_2_categories(self, numeric_property, category_property, category_property_2, df):
        df = df[df[numeric_property].notnull()]
        categories = list(set(df[category_property]))

        for category in categories:
            df_2 = df[df[category_property]==category]
            categories_2 = list(set(df_2[category_property_2])) #This is a list of all category_property_2 values for each category_property value.

            category_2_means = {}
            for category_2 in categories_2:
                df_3 = df_2[df_2[numeric_property].notnull()]
                data = list(df_3.loc[df_3[category_property_2]==category_2][numeric_property])

                if len(data) > 5:
                    category_2_means[category_2] = mean(data)

            if len(category_2_means) > 1:
                sorted_means = sorted(category_2_means.items(), key=operator.itemgetter(1), reverse=True)[0:10]
                categories_2_list = [x[0] for x in sorted_means]

                N = 0
                for category_2 in categories_2_list:
                    subset = df_2[df_2[category_property_2] == category_2]
                    N += len(subset)
                    data = subset[numeric_property].dropna().astype(float)
                    fig = sns.distplot(data, hist = False, kde = True,
                                bins = 3,
                                kde_kws = {'linewidth': 2}, label = category_2)

                    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

                plt.title(numeric_property+' for ' + category +' (N = '+str(N)+')') # You can comment this line out if you don't need title
                plt.show(fig)

    def node_record_counts(self, project_id):
        query_txt = """{node (first:-1, project_id:"%s"){type}}""" % (project_id)
        res = self.sub.query(query_txt)
        df = json_normalize(res['data']['node'])
        counts = Counter(df['type'])
        df = pd.DataFrame.from_dict(counts, orient='index').reset_index()
        df = df.rename(columns={'index':'node', 0:'count'})
        return df


    def get_data_file_tsvs(self, projects=None,remove_empty=True):
        # Download TSVs for all data file nodes in the specified projects
        #if no projects specified, get node for all projects
        if projects is None:
            projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        elif isinstance(projects, str):
            projects = [projects]
        # Make a directory for files
        mydir = 'downloaded_data_file_tsvs'
        if not os.path.exists(mydir):
            os.makedirs(mydir)
        # list all data_file 'node_id's in the data model
        dnodes = list(set(json_normalize(self.sub.query("""{_node_type (first:-1,category:"data_file") {id}}""")['data']['_node_type'])['id']))
        mnodes = list(set(json_normalize(self.sub.query("""{_node_type (first:-1,category:"metadata_file") {id}}""")['data']['_node_type'])['id']))
        inodes = list(set(json_normalize(self.sub.query("""{_node_type (first:-1,category:"index_file") {id}}""")['data']['_node_type'])['id']))
        nodes = list(set(dnodes + mnodes + inodes))
        # get TSVs and return a master pandas DataFrame with records from every project
        dfs = []
        df_len = 0
        for node in nodes:
            for project in projects:
                filename = str(mydir+'/'+project+'_'+node+'.tsv')
                if os.path.isfile(filename):
                    print('\n'+filename + " previously downloaded.")
                else:
                    prog,proj = project.split('-',1)
                    self.sub.export_node(prog,proj,node,'tsv',filename) # use the gen3sdk to download a tsv for the node
                df1 = pd.read_csv(filename, sep='\t', header=0) # read in the downloaded TSV to append to the master (all projects) TSV
                dfs.append(df1)
                df_len+=len(df1) # Counting the total number of records in the node
                print(filename +' has '+str(len(df1))+' records.')
                if remove_empty is True:
                    if df1.empty:
                        print('Removing empty file: ' + filename)
                        cmd = ['rm',filename] #look in the download directory
                        try:
                            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                        except Exception as e:
                            output = e.output.decode('UTF-8')
                            print("ERROR:" + output)
            all_data = pd.concat(dfs, ignore_index=True, sort=False)
            print('\nlength of all dfs: ' +str(df_len)) # this should match len(all_data) below
            nodefile = str('master_'+node+'.tsv')
            all_data.to_csv(str(mydir+'/'+nodefile),sep='\t')
            print('Master node TSV with '+str(len(all_data))+' total records written to '+nodefile+'.') # this should match df_len above
        return all_data

    def list_guids_in_nodes(self, nodes=None,projects=None):
        # Get GUIDs for node(s) in project(s)
        if nodes is None: # get all data_file/metadata_file/index_file 'node_id's in the data model
            categories = ['data_file','metadata_file','index_file']
            nodes = []
            for category in categories:
                query_txt = """{_node_type (first:-1,category:"%s") {id}}""" % category
                df = json_normalize(self.sub.query(query_txt)['data']['_node_type'])
                if not df.empty:
                    nodes = list(set(nodes + list(set(df['id']))))
        elif isinstance(nodes,str):
            nodes = [nodes]
        if projects is None:
            projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        elif isinstance(projects, str):
            projects = [projects]
        all_guids = {} # all_guids will be a nested dict: {project_id: {node1:[guids1],node2:[guids2]} }
        for project in projects:
            all_guids[project] = {}
            for node in nodes:
                guids=[]
                query_txt = """{%s (first:-1,project_id:"%s") {project_id file_size file_name object_id id}}""" % (node,project)
                res = self.sub.query(query_txt)
                if len(res['data'][node]) == 0:
                    print(project + ' has no records in node ' + node + '.')
                    guids = None
                else:
                    df = json_normalize(res['data'][node])
                    guids = list(df['object_id'])
                    print(project + ' has '+str(len(guids))+' records in node ' + node + '.')
                all_guids[project][node] = guids
                # nested dict: all_guids[project][node]
        return all_guids

    def get_access_token(self):
        """ get your temporary access token using your credentials downloaded from the data portal
            variable <- jsonlite::toJSON(list(api_key = keys$api_key), auto_unbox = TRUE)
            auth <- POST('https://data.braincommons.org/user/credentials/cdis/access_token', add_headers("Content-Type" = "application/json"), body = variable)

        """
        access_token = self._auth_provider._get_auth_value()
        return access_token

    def download_file_endpoint(self, guid=None):
        """ download files by getting a presigned-url from the "/user/data/download/<guid>" endpoint
        """
        if not isinstance(guid,str):
            raise Gen3Error("Please, supply GUID as string.")

        download_url = "{}/user/data/download/{}".format(self._endpoint, guid)
        print("Downloading file from '{}'.".format(download_url))

        try:
            # get the pre-signed URL
            res = requests.get(download_url, auth=self._auth_provider) # get the presigned URL
            file_url = json.loads(res.content)['url']

            # extract the filename from the pre-signed url
            f_regex = re.compile(r'.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}\/(.*)\?.*')
            fmatch = f_regex.match(res.text)
            if fmatch:
                file_name = fmatch.groups()[0]
                print("\tSaving downloaded file as '{}'".format(file_name))
            else:
                file_name = guid
                print("No matching filename in the response. Saving file with GUID as filename.")

            # get the file and write the contents to the file_name
            res_file = requests.get(file_url)
            open("./{}".format(file_name), 'wb').write(res_file.content)

        except Exception as e:
            print("\tFile '{}' failed to download: {}".format(file_name,e))

        return file_name

    def download_files_for_guids(self, guids=None, profile='profile', client='/home/jovyan/.gen3/gen3-client',method='endpoint'):
        # Make a directory for files
        mydir = 'downloaded_data_files'
        file_names = {}
        if not os.path.exists(mydir):
            os.makedirs(mydir)
        if isinstance(guids, str):
            guids = [guids]
        if isinstance(guids, list):
            for guid in guids:
                if method == 'client':
                    cmd = client+' download-single --filename-format=combined --no-prompt --profile='+profile+' --guid='+guid
                    try:
                        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
                        try:
                            file_name = re.search('Successfully downloaded (.+)\\n', output).group(1)
                            cmd = 'mv ' + file_name + ' ' + mydir
                            try:
                                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
                            except Exception as e:
                                output = e.output.decode('UTF-8')
                                print("ERROR:" + output)
                        except AttributeError:
                            file_name = '' # apply your error handling
                        print('Successfully downloaded: '+file_name)
                        file_names[guid] = file_name
                    except Exception as e:
                        output = e.output.decode('UTF-8')
                        print("ERROR:" + output)
                elif method == 'endpoint':
                    try:
                        file_name = self.download_file_endpoint(guid=guid)
                        file_names[guid] = file_name
                    except Exception as e:
                        print("Failed to download GUID {}: {}".format(guid,e))
                else:
                    print("\tPlease set method to either 'endpoint' or 'client'!".format())
        else:
            print('Provide a list of guids to download: "get_file_by_guid(guids=guid_list)"')
        return file_names

# file_name = 'GSE63878_final_list_of_normalized_data.txt.gz'
# exp.download_file_name(file_name)

    def download_file_name(self, file_name, node='datanode', project_id=None, props=['type','file_name','object_id','id','submitter_id','data_type','data_format','data_category'], all=False):
        """downloads the first file that matches a query for a file_name in a node of a project
        """
        args = 'file_name:"{}"'.format(file_name)
        response = self.paginate_query(node=node,project_id=project_id,props=props,args=args) # Use the SDK to send the query and return the response

        if 'data' in response:
            node = list(response['data'])[0]
            records = response['data'][node]

            if len(records) > 1 and all is False:
                print("\tWARNING - More than one record matched query for '{}' in '{}' node of project '{}'.".format(file_name, node, project))
                print("\t\tDownloading the first file that matched the query:\n{}".format(data[0]))

            if len(records) >= 1 and all is False:
                record = records[0]
                guid = record['object_id']
                fname = self.download_file_endpoint(guid=guid)

            elif all is True:
                guids = [record['object_id'] for record in records]
                for guid in guids:
                    self.download_file_endpoint(guid=guid)

            return records

        else:
            print("There were no records in the query for '{}' in the '{}' node of project_id '{}'".format(file_name,node,project_id))
            return response


    def get_records_for_uuids(self, uuids, project, api):
        dfs = []
        for uuid in uuids:
            #Gen3Submission.export_record("DCF", "CCLE", "d70b41b9-6f90-4714-8420-e043ab8b77b9", "json", filename="DCF-CCLE_one_record.json")
            #export_record(self, program, project, uuid, fileformat, filename=None)
            mydir = str('project_uuids/'+project+'_tsvs') #create the directory to store TSVs
            if not os.path.exists(mydir):
                os.makedirs(mydir)
            filename = str(mydir+'/'+project+'_'+uuid+'.tsv')
            if os.path.isfile(filename):
                print("File previously downloaded.")
            else:
                prog,proj = project.split('-',1)
                self.sub.export_record(prog,proj,uuid,'tsv',filename)
            df1 = pd.read_csv(filename, sep='\t', header=0)
            dfs.append(df1)
        all_data = pd.concat(dfs, ignore_index=True)
        master = str('master_uuids_'+project+'.tsv')
        all_data.to_csv(str(mydir+'/'+master),sep='\t')
        print('Master node TSV with '+str(len(all_data))+' total records written to '+master+'.')
        return all_data

    def find_duplicate_filenames(self, node, project):
        #download the node
        df = get_node_tsvs(node,project,overwrite=True)
        counts = Counter(df['file_name'])
        count_df = pd.DataFrame.from_dict(counts, orient='index').reset_index()
        count_df = count_df.rename(columns={'index':'file_name', 0:'count'})
        dup_df = count_df.loc[count_df['count']>1]
        dup_files = list(dup_df['file_name'])
        dups = df[df['file_name'].isin(dup_files)].sort_values(by='md5sum', ascending=False)
        return dups

    def get_duplicates(self, nodes, projects, api):
        # Get duplicate SUBMITTER_IDs in a node, which SHOULD NEVER HAPPEN but alas it has, thus this script
        #if no projects specified, get node for all projects
        if projects is None:
            projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        elif isinstance(projects, str):
            projects = [projects]

        # if no nodes specified, get all nodes in data commons
        if nodes is None:
            nodes = sorted(list(set(json_normalize(self.sub.query("""{_node_type (first:-1) {id}}""")['data']['_node_type'])['id'])))  #get all the 'node_id's in the data model
            remove_nodes = ['program','project','root','data_release'] #remove these nodes from list of nodes
            for node in remove_nodes:
                if node in nodes: nodes.remove(node)
        elif isinstance(nodes, str):
            nodes = [nodes]

        pdups = {}
        for project_id in projects:
            pdups[project_id] = {}
            print("Getting duplicates in project "+project_id)
            for node in nodes:
                print("\tChecking "+node+" node")
                df = paginate_query(node=node,project_id=project_id,props=['id','submitter_id'],chunk_size=1000)
                if not df.empty:
                    counts = Counter(df['submitter_id'])
                    c = pd.DataFrame.from_dict(counts, orient='index').reset_index()
                    c = c.rename(columns={'index':'submitter_id', 0:'count'})
                    dupc = c.loc[c['count']>1]
                    if not dupc.empty:
                        dups = list(set(dupc['submitter_id']))
                        uuids = {}
                        for sid in dups:
                            uuids[sid] = list(df.loc[df['submitter_id']==sid]['id'])
                        pdups[project_id][node] = uuids
        return pdups

    def delete_duplicates(self, dups, project_id, api):

        if not isinstance(dups, dict):
            print("Must provide duplicates as a dictionary of keys:submitter_ids and values:uuids; use get_duplicates function")

        program,project = project_id.split('-',1)
        failure = []
        success = []
        results = {}
        sids = list(dups.keys())
        total = len(sids)
        count = 1
        for sid in sids:
            while len(dups[sid]) > 1:
                uuid = dups[sid].pop(1)
                r = json.loads(self.sub.delete_record(program,project,uuid))
                if r['code'] == 200:
                    print("Deleted record id ("+str(count)+"/"+str(total)+"): "+uuid)
                    success.append(uuid)
                else:
                    print("Could not deleted record id: "+uuid)
                    print("API Response: " + r['code'])
                    failure.append(uuid)
            results['failure'] = failure
            results['success'] = success
            count += 1
        return results


    def query_records(self, node, project_id, api, chunk_size=500):
        # Using paginated query, Download all data in a node as a DataFrame and save as TSV
        schema = self.sub.get_dictionary_node(node)
        props = list(schema['properties'].keys())
        links = list(schema['links'])
        # need to get links out of the list of properties because they're handled differently in the query
        link_names = []
        for link in links:
            link_list = list(link)
            if 'subgroup' in link_list:
                subgroup = link['subgroup']
                for sublink in subgroup:
                    link_names.append(sublink['name'])
            else:
                link_names.append(link['name'])
        for link in link_names:
            if link in props:
                props.remove(link)
                props.append(str(link + '{id submitter_id}'))

        df = paginate_query(node,project_id,props,chunk_size)
        outfile = '_'.join(project_id,node,'query.tsv')
        df.to_csv(outfile, sep='\t', index=False, encoding='utf-8')
        return df

    # Group entities in details into succeeded (successfully created/updated) and failed valid/invalid
    def summarize_submission(self, tsv, details, write_tsvs):
        with open(details, 'r') as file:
            f = file.read().rstrip('\n')
        chunks = f.split('\n\n')
        invalid = []
        messages = []
        valid = []
        succeeded = []
        responses = []
        results = {}
        chunk_count = 1
        for chunk in chunks:
            d = json.loads(chunk)
            if 'code' in d and d['code'] != 200:
                entities = d['entities']
                response = str('Chunk ' + str(chunk_count) + ' Failed: '+str(len(entities))+' entities.')
                responses.append(response)
                for entity in entities:
                    sid = entity['unique_keys'][0]['submitter_id']
                    if entity['valid']: #valid but failed
                        valid.append(sid)
                    else: #invalid and failed
                        message = entity['errors'][0]['message']
                        messages.append(message)
                        invalid.append(sid)
                        print("Invalid record: {}\n\tmessage: {}".format(sid,message))
            elif 'code' not in d:
                responses.append('Chunk ' + str(chunk_count) + ' Timed-Out: '+str(d))
            else:
                entities = d['entities']
                response = str('Chunk ' + str(chunk_count) + ' Succeeded: '+str(len(entities))+' entities.')
                responses.append(response)
                for entity in entities:
                    sid = entity['unique_keys'][0]['submitter_id']
                    succeeded.append(sid)
            chunk_count += 1
        results['valid'] = valid
        results['invalid'] = invalid
        results['messages'] = messages
        results['succeeded'] = succeeded
        results['responses'] = responses
        submitted = succeeded + valid + invalid # 1231 in test data
        #get records missing in details from the submission.tsv
        df = pd.read_csv(tsv, sep='\t',header=0)
        missing_df = df.loc[~df['submitter_id'].isin(submitted)] # these are records that timed-out, 240 in test data
        missing = list(missing_df['submitter_id'])
        results['missing'] = missing

        # Find the rows in submitted TSV that are not in either failed or succeeded, 8 time outs in test data, 8*30 = 240 records
        if write_tsvs is True:
            print("Writing TSVs: ")
            valid_df = df.loc[df['submitter_id'].isin(valid)] # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
            invalid_df = df.loc[df['submitter_id'].isin(invalid)] # these are records that failed due to being invalid and should be reformatted
            sub_name = ntpath.basename(tsv)
            missing_file = 'missing_' + sub_name
            valid_file = 'valid_' + sub_name
            invalid_file = 'invalid_' + sub_name
            missing_df.to_csv(missing_file, sep='\t', index=False, encoding='utf-8')
            valid_df.to_csv(valid_file, sep='\t', index=False, encoding='utf-8')
            invalid_df.to_csv(invalid_file, sep='\t', index=False, encoding='utf-8')
            print('\t' + missing_file)
            print('\t' + valid_file)
            print('\t' + invalid_file)

        return results

    def write_tsvs_from_results(self,invalid_ids,filename):
            # Read the file in as a pandas DataFrame
        f = os.path.basename(filename)
        if f.lower().endswith('.csv'):
            df = pd.read_csv(filename, header=0, sep=',', dtype=str).fillna('')
        elif f.lower().endswith('.xlsx'):
            xl = pd.ExcelFile(filename, dtype=str) #load excel file
            sheet = xl.sheet_names[0] #sheetname
            df = xl.parse(sheet) #save sheet as dataframe
            converters = {col: str for col in list(df)} #make sure int isn't converted to float
            df = pd.read_excel(filename, converters=converters).fillna('') #remove nan
        elif filename.lower().endswith(('.tsv','.txt')):
            df = pd.read_csv(filename, header=0, sep='\t', dtype=str).fillna('')
        else:
            print("Please upload a file in CSV, TSV, or XLSX format.")
            exit(1)

        invalid_df = df.loc[df['submitter_id'].isin(invalid_ids)] # these are records that failed due to being invalid and should be reformatted
        invalid_file = 'invalid_' + f + '.tsv'

        print("Writing TSVs: ")
        print('\t' + invalid_file)
        invalid_df.to_csv(invalid_file, sep='\t', index=False, encoding='utf-8')

        return invalid_df

    def submit_df(self,project_id,df,chunk_size=1000,row_offset=0):
        """ Submit data in a pandas DataFrame.
        """
        df_type = list(set(df['type']))
        df.rename(
            columns={c: c.lstrip("*") for c in df.columns}, inplace=True
        )  # remove any leading asterisks in the DataFrame column names

        # Check uniqueness of submitter_ids:
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            raise Gen3Error(
                "Warning: file contains duplicate submitter_ids. \nNote: submitter_ids must be unique within a node!"
            )

        # Chunk the file
        print("Submitting {} DataFrame with {} records.".format(df_type,len(df)))
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
            ):  # time-out, response is not valid JSON at the moment

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
                        "\tInvalid records in this chunk: {}, {}".format(len(invalid), message)
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
            ):  # time-out, response is not valid JSON at the moment

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
                        "\tInvalid records in this chunk: {}, {}".format(len(invalid), message)
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

    def get_indexd(self,api,outfile=None):
        """ get all the records in indexd
            Example:
            exp.get_indexd(api='https://icgc.bionimbus.org/',outfile=True)
        """
        all_records = []
        indexd_url = "{}/index/index".format(api)
        response = requests.get(indexd_url, auth=self._auth_provider) #response = requests.get(indexd_url, auth=auth)
        records = response.json().get("records")
        all_records.extend(records)
        print("\tRetrieved {} records from indexd.".format(len(all_records)))

        previous_did = None
        start_did = records[-1].get("did")

        while start_did != previous_did:
            previous_did = start_did
            next_url = "{}?start={}".format(indexd_url,start_did)
            response = requests.get(next_url, auth=self._auth_provider) #response = requests.get(next_url, auth=auth)
            records = response.json().get("records")
            all_records.extend(records)
            print("\tRetrieved {} records from indexd.".format(len(all_records)))
            if records:
                start_did = response.json().get("records")[-1].get("did")
        if outfile:
            dc_regex = re.compile(r'https:\/\/(.+)\/')
            dc = dc_regex.match(api).groups()[0]
            outname = "{}_indexd_records.txt".format(dc)
            with open(outname, 'w') as outfile:
                outfile.write(json.dumps(all_records))
        return all_records

    def get_urls(self, guids, api):
        # Get URLs for a list of GUIDs
        if isinstance(guids, str):
            guids = [guids]
        if isinstance(guids, list):
            urls = {}
            for guid in guids:
                index_url = "{}/index/{}".format(api, guid)
                output = requests.get(index_url, auth=self._auth_provider).text
                guid_index = json.loads(output)
                url = guid_index['urls'][0]
                urls[guid] = url
        else:
            print("Please provide one or a list of data file GUIDs: get_urls\(guids=guid_list\)")
        return urls

    def get_guids_for_file_names(self, file_names, method='indexd',match='file_name'):
        # Get GUIDs for a list of file_names
        if isinstance(file_names, str):
            file_names = [file_names]
        if not isinstance(file_names,list):
            print("Please provide one or a list of data file file_names: get_guid_for_filename\(file_names=file_name_list\)")
        guids = {}
        if method == 'indexd':
            for file_name in file_names:
                index_url = "{}/index/index/?file_name={}".format(self._endpoint,file_name)
                response = requests.get(index_url, auth=self._auth_provider).text
                index_record = json.loads(response)
                if len(index_record['records']) > 0:
                    guid = index_record['records'][0]['did']
                    guids[file_name] = guid
        elif method == 'sheepdog':
            for file_name in file_names:
                if match == 'file_name':
                    args = 'file_name:"{}"'.format(file_name)
                elif match == 'submitter_id':
                    args = 'submitter_id:"{}"'.format(file_name)
                props = ['object_id']
                res = self.paginate_query(node='datanode',args=args,props=props)
                recs = res['data']['datanode']
                if len(recs) >= 1:
                    guid = recs[0]['object_id']
                    guids[file_name] = guid
                else:
                    print("Found no sheepdog records with {}: {}".format(method,file_name))
                if len(recs) > 1:
                    guids = [rec['object_id'] for rec in recs]
                    guids[file_name] = guids
                    print("Found more than 1 sheepdog record with {}: {}".format(method,file_name))
        else:
            print("Enter a valid method.\n\tValid methods: 'sheepdog','indexd'")
        return guids

    def write_manifest_for_guids(self,guids,filename="gen3_manifest.json"):
        """ write a gen3-client manifest from provided list of guids
        """

        with open(filename, 'w') as manifest:

            manifest.write('[')

            count = 0
            for guid in guids:
                count += 1
                manifest.write('\n\t{')
                manifest.write('"object_id": "{}"'.format(guid))

                if count == len(guids):
                    manifest.write('  }]')
                else:
                    manifest.write('  },')

                print("\t{} ({}/{})".format(guid,count,len(guids)))

            print("\tDone ({}/{}).".format(count,len(guids)))
            print("\tManifest written to file: {}".format(filename))

# guids = exp.get_guids_for_file_names(file_names=file_names,method='sheepdog',match='submitter_id')

    def get_index_for_file_names(self, file_names):
        # Get GUIDs for a list of file_names
        if isinstance(file_names, str):
            file_names = [file_names]
        if not isinstance(file_names,list):
            print("Please provide one or a list of data file file_names: get_guid_for_filename\(file_names=file_name_list\)")
        index_records = []
        for file_name in file_names:
            index_url = "{}/index/index/?file_name={}".format(self._endpoint,file_name)
            response = requests.get(index_url, auth=self._auth_provider).text
            index_records.append(json.loads(response))

        return index_records

    def get_index_for_url(self, url, api):
        """ Returns the indexd record for a file's storage location URL ('urls' in indexd)
            Example:
                api='https://icgc.bionimbus.org/'
                url='s3://pcawg-tcga-sarc-us/2720a2b8-3f4e-5b6e-9f74-1067a068462a'
                exp.get_record_for_url(url=url,api=api)
        """
        indexd_endpoint = "{}/index/index/".format(api)
        indexd_query = "{}?url={}".format(indexd_endpoint,url)
        output = requests.get(indexd_query, auth=self._auth_provider).text
        response = json.loads(output)
        index_records = response['records']
        return index_records

    def get_index_for_guids(self, guids):
        """ Returns the indexd record for a GUID ('urls' in indexd)
        """
        if isinstance(guids, str):
            guids = [guids]

        index_records = []
        for guid in guids:
            print("\tGetting index for GUID ({}/{}): {}".format(len(index_records),len(guids),guid))
            indexd_endpoint = "{}/index/index/".format(self._endpoint)
            indexd_query = "{}{}".format(indexd_endpoint,guid)
            response = requests.get(indexd_query, auth=self._auth_provider).text
            records = json.loads(response)
            index_records.append(records)
        return index_records

# failed = [irec for irec in irecs if irec['size'] is None]
# failed_guids = [irec['did'] for irec in failed]

    def get_guid_for_url(self, url, api):
        """Return the GUID for a file's URL in indexd
            Example:
                api='https://icgc.bionimbus.org/'
                url='s3://pcawg-tcga-sarc-us/2720a2b8-3f4e-5b6e-9f74-1067a068462a'
                exp.get_guid_for_url(url=url,api=api)
        """
        index_records = self.get_record_for_url(url=url,api=api)
        if len(index_records) == 1:
            guid = index_records[0]['did']
            return guid
        else:
            guids = []
            for index_record in index_records:
                guids.append(index_record['did'])
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

        for guid in guids:

            fence_url = "{}user/data/".format(
                self._endpoint
            )

            try:
                response = requests.delete(
                    fence_url + guid,
                    auth=self._auth_provider
                )
            except requests.exceptions.ConnectionError as e:
                raise Gen3Error(e)

            if (response.status_code == 204):
                print("Successfully deleted GUID {}".format(guid))
            else:
                print("Error deleting GUID {}:".format(guid))
                print(response.reason)

    def uploader_index(self, uploader='cgmeyer@uchicago.edu', acl=None, limit=1024, format='guids'):
        """Get records from indexd of the files uploaded by a particular user.

        Args:
            uploader (str): The uploader's data commons login email.

        Examples:
            This returns all records of files that I uploaded to indexd.

            >>> Gen3Submission.submit_file(uploader="cgmeyer@uchicago.edu")
            #data.bloodpac.org/index/index/?limit=1024&acl=null&uploader=cgmeyer@uchicago.edu
        """

        if acl is not None:
            index_url = "{}/index/index/?limit={}&acl={}&uploader={}".format(
                self._endpoint,limit,acl,uploader
            )
        else:
            index_url = "{}/index/index/?limit={}&uploader={}".format(
                self._endpoint,limit,uploader
            )
        try:
            response = requests.get(
                index_url,
                auth=self._auth_provider
            ).text
        except requests.exceptions.ConnectionError as e:
            print(e)

        try:
            data = json.loads(response)
        except JSONDecodeError as e:
            print(response)
            print(str(e))
            raise Gen3Error("Unable to parse indexd response as JSON!")

        records = data['records']

        if records is None:
            print("No records in the index for uploader {} with acl {}.".format(uploader,acl))

        elif format is 'tsv':
            df = json_normalize(records)
            filename = "indexd_records_for_{}.tsv".format(uploader)
            df.to_csv(filename,sep='\t',index=False, encoding='utf-8')
            return df

        elif format is 'guids':
            guids = []
            for record in records:
                guids.append(record['did'])
            return guids

        else:
            return records


## To do
#
# filter indexd by project:
# https://data.braincommons.org/index/index/?acl=Project1
# https://data.braincommons.org/index/index/?acl=Program1
# https://data.braincommons.org/index/index/?acl=Program1,Project1 #doesn't work for some reason

# # find indexd records by upload date:
# /index/index/?acl=null&uploader=cgmeyer@uchicago.edu
#
# api = 'https://vpodc.org/'
# uploader = 'cgmeyer@uchicago.edu'
# index_url = api + '/index/index/?limit=200&acl=null&uploader='+uploader
# output = requests.get(index_url, auth=auth).text
# index_record = json.loads(output)
# index_record
#
# latest=[]
# guids = []
# records = index_record['records']
# for record in records:
#     if '2019-06' in record['updated_date'] or '2019-05-31' in record['updated_date']:
#         print(record)
#         latest.append(record)
#         guids.append(record['did'])
# len(latest)
# len(guids)
#
# # add index search by md5
# https://data.bloodpac.org/index/index/?hash=md5:14c626a4573f2d8e2a1cf796df68a4b8
#
# ## add index stats
# api/index/_stats
#
# ## Add a check authentication command to Gen3sdk:
#
# user_endpoint = api + '/user/user/'

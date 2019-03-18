## Gen3 SDK Expansion pack

import requests, json, fnmatch, os, os.path, sys, subprocess, glob
import pandas as pd
from pandas.io.json import json_normalize

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

endpoint = 'https://my.datacommons.org'
auth = Gen3Auth(endpoint, refresh_file='my-credentials.json')
sub = Gen3Submission(endpoint, auth)
file = Gen3File(endpoint, auth)

### AWS S3 Tools:
def s3_ls(path, bucket, profile, pattern='*'):
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

def s3_files(path, bucket, profile, pattern='*',verbose=True):
    ''' Get a list of files returned by an `aws s3 ls` command '''
    s3_path = bucket + path
    cmd = ['aws', 's3', 'ls', s3_path, '--profile', profile]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
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

def get_s3_files(path, bucket, profile, files=None, mydir=None):
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
                  output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
              except Exception as e:
                  output = e.output.decode('UTF-8')
                  print("ERROR:" + output)
    # If files is None, which syncs the s3_path 'directory'
    else:
       print("Syncing directory " + s3_path)
       cmd = ['aws', 's3', '--profile', profile, 'sync', s3_path, mydir]
       try:
          output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
       except Exception as e:
          output = e.output.decode('UTF-8')
          print("ERROR:" + output)
    print("Finished")



# Functions for downloading metadata in TSVs
# Get a list of project_ids
def get_project_ids(node=None,name=None):
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
        df = json_normalize(sub.query(query)['data']['node'])
        project_ids = project_ids + list(set(df['project_id']))
    if len(queries) > 0:
        for query in queries:
            res = sub.query(query)
            df = json_normalize(res['data']['project'])
            project_ids = project_ids + list(set(df['project_id']))
    return sorted(project_ids,key=str.lower)


# Create master TSV of data from each project per node
def get_node_tsvs(node,projects=None):
    #Get a TSV of the node(s) specified for each project specified
    if not isinstance(node, str): # Create folder on VM for downloaded files
        mydir = 'downloaded_tsvs'
    else:
        mydir = str(node+'_tsvs')
    if not os.path.exists(mydir):
        os.makedirs(mydir)
    if projects is None: #if no projects specified, get node for all projects
        project_ids = list(json_normalize(sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
    dfs = []
    df_len = 0
    for project in projects:
        filename = str(mydir+'/'+project+'_'+node+'.tsv')
        if os.path.isfile(filename):
            print("File previously downloaded.")
        else:
            prog,proj = project.split('-')
            sub.export_node(prog,proj,node,'tsv',filename)
        df1 = pd.read_csv(filename, sep='\t', header=0)
        dfs.append(df1)
        df_len+=len(df1)
        print(filename +' has '+str(len(df1))+' records.')
    all_data = pd.concat(dfs, ignore_index=True)
    print('length of all dfs: ' +str(df_len))
    nodefile = str('master_'+node+'.tsv')
    all_data.to_csv(str(mydir+'/'+nodefile),sep='\t')
    print('Master node TSV with '+str(len(all_data))+' total records written to '+nodefile+'.')
    return all_data


def get_project_tsvs(projects):
    # Get a TSV for every node in a project
    all_nodes = list(set(json_normalize(sub.query("""{_node_type (first:-1) {id}}""")['data']['_node_type'])['id']))  #get all the 'node_id's in the data model
    if isinstance(projects,str):
        projects = [projects]
    for project_id in projects:
        #create the directory to store TSVs
        mydir = str('project_tsvs/'+project_id+'_tsvs')
        if not os.path.exists(mydir):
            os.makedirs(mydir)
        for node in all_nodes:
            #check if the project has records in the node
            res = sub.query("""{node (of_type:"%s", project_id:"%s"){project_id}}""" % (node,project_id))
            df = json_normalize(res['data']['node'])
            if not df.empty:
                filename = str(mydir+'/'+project_id+'_'+node+'.tsv')
                if os.path.isfile(filename):
                    print("File previously downloaded.")
                else:
                    prog,proj = project_id.split('-',1)
                    sub.export_node(prog,proj,node,'tsv',filename)
                    print(filename+' exported to '+mydir)
    cmd = ['ls',mydir]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
    except Exception as e:
        output = e.output.decode('UTF-8')
        print("ERROR:" + output)
    return output


def delete_node(node,project_id):
    failure = []
    success = []
    other = []
    results = {}

    program,project = project_id.split('-',1)

    query = """{_%s_count (project_id:"%s") %s (first: 0, project_id:"%s"){id}}""" % (node,project_id,node,project_id)

    res = sub.query(query)
    ids = [x['id'] for x in res['data'][node]]

    for uuid in ids:
        r = json.loads(sub.delete_record(program,project,uuid))
        if r['code'] == 400:
            failure.append(uuid)
        elif r['code'] == 200:
            success.append(uuid)
        else:
            other.append(uuid)
    results['failure'] = failure
    results['success'] = success
    results['other'] = other
    return results





def get_urls(guids,api):
    if isinstance(guids, str):
        guids = [guids]
    if isinstance(guids, list):
        urls = {}
        for guid in guids:
            index_url = "{}index/{}".format(api, guid)
            output = requests.get(index_url, auth=auth).text
            guid_index = json.loads(output)
            url = guid_index['urls'][0]
            urls[guid] = url
    else:
        print("Please provide one or a list of data file GUIDs: get_urls\(guids=guid_list\)")
    return urls


def delete_uploaded_files(guids,api):
# DELETE http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/fence/master/openapis/swagger.yaml#/data/delete_data__file_id_
# ​/data​/{file_id}
# delete all locations of a stored data file and remove its record from indexd.
# After a user uploads a data file and it is registered in indexd,
# but before it is mapped into the graph via metadata submission,
# this endpoint will delete the file from its storage locations (saved in the record in indexd)
# and delete the record in indexd.
    if isinstance(guids, str):
        guids = [guids]
    if isinstance(guids, list):
    for guid in guids:
        fence_url = api + 'user/data/'
        response = requests.delete(fence_url + guid,auth=auth)
        if (response.status_code == 204):
            print("Successfully deleted GUID {}".format(guid))
        else:
            print("Error deleting GUID {}:".format(guid))
            print(response.reason)



def delete_records(uuids,project_id):
    ## Delete a list of records in 'uuids' from a project
    program,project = project_id.split('-',1)
    failure = []
    success = []
    other = []
    results = {}
    if isinstance(uuids, str):
        uuids = [uuids]
    if isinstance(uuids, list):
        for uuid in uuids:
            r = json.loads(sub.delete_record(program,project,uuid))
            if r['code'] == 400:
                failure.append(uuid)
            elif r['code'] == 200:
                success.append(uuid)
            else:
                other.append(uuid)
    results['failure'] = failure
    results['success'] = success
    results['other'] = other
    return results

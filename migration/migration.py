import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re, operator
from shutil import copyfile
copyfile(src, dst)
import pandas as pd
from pandas.io.json import json_normalize
from collections import Counter
from statistics import mean

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion')
from expansion import Gen3Expansion

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

class Gen3Error(Exception):
    pass

class Gen3Migration:
    """Scripts for migrating a Data Commons DB using TSV templates.

    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Migration class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... mig = Gen3Migration(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint
        self.sub = Gen3Submission(endpoint, auth_provider)
        self.sub = Gen3Expansion(endpoint, auth_provider)

    def get_project_ids(self, node=None,name=None):
        """Get a list of project_ids to have access to in data commons.

            Args:
                node(str): The node you want projects to have at least one record in.
                name(str): The name of the programs to get projects in.

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
        return sorted(project_ids,key=str.lower)

    def get_node_tsvs(self, node, projects=None, overwrite=False, remove_empty=True):
        """Gets a TSV of the structuerd data from particular node for each project specified.
           Also creates a master TSV of merged data from each project for the specified node.
           Returns a DataFrame containing the merged data for the specified node.

        Args:
            node (str): The name of the node to download structured data from.
            projects (list): The projects to download the node from. If "None", downloads data from each project user has access to.

        Example:
        >>> df = get_node_tsvs('demographic')

        """
        if not isinstance(node, str): # Create folder on VM for downloaded files
            mydir = 'downloaded_tsvs'
        else:
            mydir = str(node+'_tsvs')

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

    def get_project_tsvs(self, projects=None, overwrite=False):
        """Function gets a TSV for every node in a specified project.
            Exports TSV files into a directory "project_tsvs/".
            Function returns a list of the contents of the directory.

        Args:
            projects (str/list): The project_id(s) of the project(s) to download. Can be a single project_id or a list of project_ids.
            overwrite (boolean): If False, the TSV file is not downloaded if there is an existing file with the same name.

        Example:
        >>> get_project_tsvs(projects = ['internal-test'])

        """
        all_nodes = sorted(list(set(json_normalize(self.sub.query("""{_node_type (first:-1) {id}}""")['data']['_node_type'])['id'])))  #get all the 'node_id's in the data model
        remove_nodes = ['program','project','root','data_release'] #remove these nodes from list of nodes
        for node in remove_nodes:
            if node in all_nodes: all_nodes.remove(node)

        if projects is None: #if no projects specified, get node for all projects
            projects = list(json_normalize(self.sub.query("""{project (first:0){project_id}}""")['data']['project'])['project_id'])
        elif isinstance(projects, str):
            projects = [projects]

        for project_id in projects:
            mydir = str('project_tsvs/'+project_id+'_tsvs') #create the directory to store TSVs

            if not os.path.exists(mydir):
                os.makedirs(mydir)

            for node in all_nodes:
                query_txt = """{_%s_count (project_id:"%s")}""" % (node,project_id)
                res = self.sub.query(query_txt)
                count = res['data'][str('_'+node+'_count')]
                print(str(count) + ' records found in node ' + node + ' in project ' + project_id)

                if count > 0:
                    filename = str(mydir+'/'+project_id+'_'+node+'.tsv')

                    if (os.path.isfile(filename)) and (overwrite is False):
                        print('Previously downloaded '+ filename )

                    else:
                        prog,proj = project_id.split('-',1)
                        self.sub.export_node(prog,proj,node,'tsv',filename)

        cmd = ['ls',mydir] #look in the download directory
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
        except Exception as e:
            output = 'ERROR:' + e.output.decode('UTF-8')

        return output

####################################################################################
####################################################################################
####################################################################################
####################################################################################

def make_temp_files(prefix,suffix):
    pattern = "{}*{}".format(prefix,suffix)
    file_names = glob.glob(pattern)
    for file_name in file_names:
        temp_name = "temp_{}".format(file_name)
        copyfile(file_name, temp_name)

make_temp_files(prefix='DEV',suffix='tsv')

nodes = ['imaging_fmri_exam',
    'imaging_mri_exam',
    'imaging_spect_exam',
    'imaging_ultrasonography_exam',
    'imaging_xray_exam',
    'imaging_ct_exam',
    'imaging_pet_exam']

def merge_nodes(in_nodes,out_node):
    """
    Merges a list of node TSVs into a single TSV.
    """
    dfs = []
    for node in in_nodes:
        filename = "temp_{}_{}.tsv".format(project_id, node)
        try:
            df1 = pd.read_csv(filename,sep='\t', header=0, dtype=str)
            dfs.append(df1)
            print("{} node found with {} records.".format(node,len(df1)))
        except IOError as e:
            print("Can't read file {}".format(filename))
            pass
    df = pd.concat(dfs, ignore_index=True)
    df['type'] = out_node
    outname = "temp_{}_{}.tsv".format(project_id, out_node)
    df.to_csv(outname, sep='\t', index=False, encoding='utf-8')
    print("Total of {} records written to node {} in file {}.".format(len(df),out_node,outname))
    return df

df=merge_nodes(in_nodes=nodes,out_node='imaging_exam')


node = 'imaging_exam'
link = 'visit'
properties = ['visit_label','visit_method']
values = ['Imaging','In-person Visit']

def add_missing_links(node,link):
    """
    This function adds missing links to a node's TSV when the parent node changes.
    Args:
        node (str): This is the node TSV to add links to.
        link (str): This is the name of the node to add links to.
    Example:
        This adds missing links to the visit node to the imaging_exam TSV.
        add_missing_links('imaging_exam','visit')
    """
    filename = "temp_{}_{}.tsv".format(project_id, node)
    df = pd.read_csv(filename,sep='\t', header=0, dtype=str)
    link_name = "{}s.submitter_id".format(link)
    df_no_link = df.loc[df[link_name].isnull()] # imaging_exams with no visit
    if len(df_no_link) > 0:
        df_no_link[link_name] = df_no_link['submitter_id'] + "_{}".format(link) # visit submitter_id is "<ESID>_visit"
        #df_no_link = df_no_link.drop(columns=['visits.id'])
        # Merge dummy visits back into original df
        df_link = df.loc[df[link_name].notnull()]
        df_final = pd.concat([df_link,df_no_link],ignore_index=True,sort=False)
        df_final.to_csv(filename, sep='\t', index=False, encoding='utf-8')
        print("{} links to node {} added to for {} in TSV file: {}".format(str(len(df_no_link)),link,node,filename))
    else:
        print("No records are missing links to {} in the {} TSV.".format(link,node))
    return df_final


node = 'imaging_exam'
link = 'visit'
old_parent = 'case'
properties = {'visit_label':'Imaging','visit_method':'In-person Visit'}

def create_missing_links(node,link,old_parent,properties):
    """
    This fxn creates links TSV for links in a node that don't exist.

    Args:
        node(str): This is the node TSV in which to look for links that don't exist.
        link(str): This is the node to create the link records in.
        old_parent(str): This is the parent node of 'node' prior to the dictionary change.
        properties(dict): Dict of required properties/values to add to new link records.
    Example:
        This will create visit records that don't exist in the visit TSV but are in the imaging_exam TSV.
    """
    filename = "temp_{}_{}.tsv".format(project_id,node)
    df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
    link_name = "{}s.submitter_id".format(link)
    link_names = list(df[link_name])
    link_file = "temp_{}_{}.tsv".format(project_id,link)
    try:
        link_df = pd.read_csv(link_file,sep='\t',header=0,dtype=str)
        existing = list(link_df['submitter_id'])
        missing = set(link_names).difference(existing) #lists items in link_names missing from existing
        if len(missing) > 0:
            print("Creating {} records in {} node for missing {} links.".format(len(missing),link,node))
        else:
            print("All {} records in {} node have a link to {}. No new records added.".format(len(existing),node,link))
            return link_df
    except FileNotFoundError as e:
        link_df = pd.DataFrame()
        print("No existing {} TSV found. Creating new TSV for links.".format(link))
        missing = link_names
    parent_link = "{}s.submitter_id".format(old_parent)
    new_links = df.loc[df[link_name].isin(missing)][[link_name,parent_link]]
    new_links = new_links.rename(columns={link_name:'submitter_id'})
    new_links['type'] = link
    for prop in properties:
        new_links[prop] = properties[prop]
    all_links = pd.concat([link_df,new_links],ignore_index=True)
    all_links.to_csv(link_file,sep='\t',index=False,encoding='utf-8')
    print("{} new missing {} records saved into TSV file: {}".format(str(len(new_links)),link,link_file))
    return all_links

create_missing_links(node,link,old_parent,properties)



properties = ['days_to_preg_serum','days_to_urine_dip','preg_not_required','preg_test_performed','serum_pregnancy_test_performed','serum_pregnancy_test_result','preg_serum_time','urine_pregnancy_dip_performed','urine_pregnancy_dip_result','urine_dip_time']
properties = ['days_to_preg_serum']
to_node = 'reproductive_health'
from_node = 'imaging_exam'
parent_node = 'visit'

def move_properties(from_node,to_node,parent_node='visit',properties):
    """
    This function takes a node with properties to be moved (from_node) and moves those properties/data to a new node (to_node).
    Fxn also checks whether the data for proeprties to be moved actually has data. If not, no new records are created.
    """
    # Warn if there are from_node records with no links to parent_node,
    # e.g., imaging_exam records with no links to visit.
    from_name = "temp_{}_{}.tsv".format(project_id,from_node)
    df_from = pd.read_csv(from_name,sep='\t',header=0,dtype=str)
    to_name = "temp_{}_{}.tsv".format(project_id,to_node)
    try:
        df_to = pd.read_csv(to_name,sep='\t',header=0,dtype=str)
    except FileNotFoundError as e:
        df_to = pd.DataFrame()
        print("No existing {} TSV found. Creating new TSV for data to be moved.".format(to_node))

    parent_link = "{}s.submitter_id".format(parent_node)
    from_no_link = df_from.loc[df_from[parent_link].isnull()] # from_node records with no link to parent_node
    if not from_no_link.empty: # if there are records with no links to parent node
        print("Warning: there are {} {} records with no links to {} node!".format(len(from_no_link),from_node,parent_node))
    else:
        # only create reproductive_health nodes if they aren't all null
        proceed = False
        for prop in properties:
            if len(df_from.loc[df_from[prop].notnull()]) > 0:
                proceed = True
        if proceed:
            # create the reproductive_health nodes based on imaging exams
            all_props = [parent_link] + properties
            new_to = df_from[all_props]
            new_to['type'] = to_node
            new_to['project_id'] = project_id
            new_to['submitter_id'] = df_from['submitter_id'] + "_reproductive_health"

            # Write reproductive_health records:
            # Check if to_name already exists
            all_to = pd.concat([df_to,new_to],ignore_index=True)
            all_to.to_csv(to_name,sep='\t',index=False,encoding='utf-8')

            print("New {} records created from properties moved from the {} TSV written to file:\n\t{}".format(to_node,from_node,to_name))
        else:
            print("No non-null {} data found in {} records. No reproductive health TSV written.".format(to_node,from_node))


            # change some column names to new prop name
            new_to.rename(columns = {'days_to_preg_serum':'days_to_serum_pregnancy_test',
                'days_to_urine_dip':'days_to_urine_pregnancy_dip',
                'preg_not_required':'pregnancy_test_not_required',
                'preg_test_performed':'pregnancy_test_performed',
                'preg_serum_time':'serum_pregnancy_test_time',
                'urine_dip_time':'urine_pregnancy_dip_time'},
                inplace = True)

        # drop migrated properties from imaging_exam and
        try:
            df = df.drop(columns=['days_to_preg_serum','days_to_urine_dip',
                'preg_not_required','preg_test_performed',
                'serum_pregnancy_test_performed','serum_pregnancy_test_result',
                'preg_serum_time','urine_pregnancy_dip_performed',
                'urine_pregnancy_dip_result','urine_dip_time'])
            df = drop_links(df,links=['cases','diagnoses','followups'])
            print("Deprecated links removed from imaging_exams df.")
            outname = write_node_df(project_id=project_id, node='imaging_exam', df=df)
            print("Writing migrated imaging_exam data to: {}".format(outname))
        except Exception as e:
            print("Couldn't drop links from imaging exams: {}".format(e))
    return df

move_properties(to_node,from_node,properties)




def read_nodes():

def write_nodes():



def drop_properties():

def drop_links():


# def add_property():

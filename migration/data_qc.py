import os, os.path, sys, subprocess, glob, json, datetime, collections
import fnmatch, sys, ntpath, copy, re, operator, requests, statistics
from shutil import copyfile
from pathlib import Path
from collections import Counter
from statistics import mean
from operator import itemgetter
import numpy as np
from scipy import stats
import pandas as pd
from pandas.io.json import json_normalize
pd.options.mode.chained_assignment = None # turn off pandas chained assignment warning
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

"""
In an interactive python environment

# Using Pip:
#import gen3
#from gen3.auth import Gen3Auth
#from gen3.submission import Gen3Submission

#
# Download gen3sdk scripts from GitHub
# !wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
# %run ./submission.py
# sub = Gen3Submission(api, auth)
#
sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/gen3')
from submission import Gen3Submission
from auth import Gen3Auth


#
# Download additional scripts from GitHub:
# !wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
# %run ./expansion.py
# exp = Gen3Expansion(api, auth)
#
sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion')
from expansion import Gen3Expansion

# Create instances of the sdk classes for each Data Commons environment (for example Staging, QA and Production)
# BRAIN Commons Data Staging
api = 'https://data-staging.braincommons.org/'
profile = 'brain-staging'
creds = '/Users/christopher/Downloads/brain-staging-credentials.json'
auth = Gen3Auth(api,refresh_file=creds)
sub = Gen3Submission(api,auth)
exp = Gen3Expansion(api, auth)

# BRAIN Commons Production
prod_api = 'https://data.braincommons.org/'
prod_profile = 'bc'
prod_creds = '/Users/christopher/Downloads/bc-credentials.json'
prod_auth = Gen3Auth(prod_api,refresh_file=prod_creds)
prod_sub = Gen3Submission(prod_api,prod_auth)
prod_exp = Gen3Expansion(prod_api, prod_auth)


1) Download the data from a commons using the Gen3sdk

home_dir = "/Users/christopher/Documents/Notes/BHC/data_qc/"
os.chdir(home_dir)

exp.get_project_tsvs(projects=project_ids,outdir='staging_tsvs',overwrite=False)
prod_exp.get_project_tsvs(projects=project_ids,outdir='prod_tsvs',overwrite=False)

staging_dir = "/Users/christopher/Documents/Notes/BHC/data_qc/staging_tsvs/"
sdd = sub.get_dictionary_all()

prod_dir = "/Users/christopher/Documents/Notes/BHC/data_qc/prod_tsvs/"
pdd = prod_sub.get_dictionary_all()

commons = {"staging":{"dir":staging_dir,"dd":sdd},"prod":{"dir":prod_dir,"dd":pdd}} # for each commons, give directory containing the project_tsvs and the data dictionary.

"""

def t(var):
    vtype = type(var)
    print(vtype)
    if vtype in [dict,list]:
        print("{}".format(list(var)))
    if vtype in [str,int,float]:
        print("{}".format(var))

def get_output_name(name,extension,commons,outdir='reports'):
    outname = "{}_".format(name)
    for i in range(len(dcs)):
        outname += dcs[i]
        if i != len(dcs)-1:
            outname += '_'
    outname += '.{}'.format(extension)
    outname = "{}/{}".format(outdir,outname)#reports/summary_staging_prod.tsv
    return outname

def create_output_dir(outdir='reports'):
    cmd = ['mkdir','-p',outdir]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
    except Exception as e:
        output = e.output.decode('UTF-8')
        print("ERROR:" + output)
    return outdir

def list_links(link_list,commons):
    """
    Take node's 'links' definition (commons[dcs[0]]['dd'][shared_node]['links']), which is a list of dicts, and return a list of indiv link names.
    """
    link_names = []
    for link in link_list:#link=link_list[0]
        if 'subgroup' in link:
            sublinks = list(link['subgroup'])
            for sublink in sublinks:
                link_names.append(sublink['name'])
        else:
            link_names.append(link['name'])
    return link_names

def get_prop_type(node,prop,dc,commons):
    prop_def = commons[dc]['dd'][node]['properties'][prop]
    if 'type' in prop_def:
        prop_type = prop_def['type']
    elif 'enum' in prop_def:
        prop_type = 'enum'
    elif 'oneOf' in prop_def:
        prop_type = prop_def['oneOf']
    elif 'anyOf' in prop_def:
        prop_type = prop_def['anyOf']
    else:
        print("Can't get the property type for {} in {}!".format(shared_prop,dc))
    return prop_type


def summarize_dd(commons,props_to_remove=['case_submitter_id'],nodes_to_remove=['root','metaschema']):
    """
    Calculates summary statistics for TSVs downloaded from a data commons using the SDK function Gen3Expansion.get_project_tsvs()
    Args:
        commons(dict): Dict of data commons, has keys 'dir', directory of TSVs, and 'dd', the data dictionary obtained via gen3sdk function dd = Gen3Submission.get_dictionary_all()
        props_to_remove(list): Properties to exclude from the summary.
        nodes_to_remove(list): Items in data dictionary to exclude from summary, e.g., root and metaschema.
        outdir(str):
        home_dir(str):
    Examples:

    """
    dds = {}
    nodes = []
    node_regex = re.compile(r'^[^_][A-Za-z0-9_]+$')# don't match _terms,_settings,_definitions, etc.)

    # Get lists of nodes for each commons using their data dict
    dcs = list(commons.keys())
    for dc in dcs:
        dd = commons[dc]['dd']
        nodes = list(filter(node_regex.search, list(dd)))
        dds[dc] = {}
        for node in nodes:
            dds[dc][node] = []
            props = list(commons[dc]['dd'][node]['properties'])
            for prop in props:
                dds[dc][node].append(prop)

    # Compare the data dictionaries of two data commons in list of commons.keys()
    if len(commons) > 1:
        nodes0 = list(dds[dcs[0]])
        nodes1 = list(dds[dcs[1]])
        diff1 = set(nodes0).difference(nodes1)
        diff2 = set(nodes1).difference(nodes0)
        msg1 = "{} nodes in '{}' missing from '{}'".format(len(diff1),dcs[0],dcs[1])
        msg2 = "{} nodes in '{}' missing from '{}'".format(len(diff2),dcs[1],dcs[0])
        dds['node_diffs'] = {msg1:diff1,msg2:diff2}
        dds['node_diffs']

        shared_nodes = list(set(nodes0).intersection(nodes1))
        for shared_node in shared_nodes:
            for node_to_remove in nodes_to_remove:
                if node_to_remove in shared_nodes:
                    shared_nodes.remove(node_to_remove)

        dds['prop_diffs'] = {}
        for shared_node in shared_nodes:

            dds['prop_diffs'][shared_node] = {}

            # LINK_DIFFS: get difference in shared node link lists; sample node in prod has both subgroup and indiv links (good for testing)
            # this information is actually captured in 'missing_props' since link names are included in the list of properties.
            link_list0 = commons[dcs[0]]['dd'][shared_node]['links']
            link_list1 = commons[dcs[1]]['dd'][shared_node]['links']
            links0 = list_links(link_list0,commons)
            links1 = list_links(link_list1,commons)
            diff1 = set(links0).difference(links1)
            diff2 = set(links1).difference(links0)
            if len(list(diff1)+list(diff2)) > 0:
                dds['prop_diffs'][shared_node]['link_diffs'] = []
                msg1 = "{} links in '{}' '{}' node missing from '{}'".format(len(diff1),dcs[0],shared_node,dcs[1])
                msg2 = "{} links in '{}' '{}' node missing from '{}'".format(len(diff2),dcs[1],shared_node,dcs[0])
                dds['prop_diffs'][shared_node]['link_diffs'].append([(msg1,diff1),(msg2,diff2)])

            # PROP_DIFFS: get difference in shared node property lists
            props0 = list(dds[dcs[0]][shared_node])
            props1 = list(dds[dcs[1]][shared_node])
            for prop_list in [props0,props1]:
                for prop_to_remove in props_to_remove:
                    if prop_to_remove in prop_list:
                        prop_list.remove(prop_to_remove)
            diff1 = set(props0).difference(props1)
            diff2 = set(props1).difference(props0)
            if len(list(diff1)+list(diff2)) > 0:
                dds['prop_diffs'][shared_node]['missing_props'] = []
                msg1 = "{} properties in '{}' '{}' node missing from '{}'".format(len(diff1),dcs[0],shared_node,dcs[1])
                msg2 = "{} properties in '{}' '{}' node missing from '{}'".format(len(diff2),dcs[1],shared_node,dcs[0])
                dds['prop_diffs'][shared_node]['missing_props'].append([(msg1,diff1),(msg2,diff2)])

            # ENUM_DIFFS: get differences in shared node's properties' enumerations
            shared_props = list(set(props0).intersection(props1))
            dds['prop_diffs'][shared_node]['type_changes'] = {}
            dds['prop_diffs'][shared_node]['enum_diffs'] = {}
            for shared_prop in shared_props:

                prop_type0 = get_prop_type(shared_node,shared_prop,dcs[0],commons=commons)
                prop_type1 = get_prop_type(shared_node,shared_prop,dcs[1],commons=commons)

                if prop_type0 != prop_type1:
                    dds['prop_diffs'][shared_node]['type_changes'][shared_prop] = [(dcs[0],prop_type0),(dcs[1],prop_type1)]

                elif prop_type0 == 'enum':
                    enums0 = list(commons[dcs[0]]['dd'][shared_node]['properties'][shared_prop]['enum'])
                    enums1 = list(commons[dcs[1]]['dd'][shared_node]['properties'][shared_prop]['enum'])
                    diff1 = set(enums0).difference(enums1)
                    diff2 = set(enums1).difference(enums0)
                    if len(list(diff1)+list(diff2)) > 0:
                        dds['prop_diffs'][shared_node]['enum_diffs'][shared_prop] = []
                        msg1 = "{} enums in '{}' '{}' node missing from '{}'".format(len(diff1),dcs[0],prop,dcs[1])
                        msg2 = "{} enums in '{}' '{}' node missing from '{}'".format(len(diff2),dcs[1],prop,dcs[0])
                        dds['prop_diffs'][shared_node]['enum_diffs'][shared_prop].append([(msg1,diff1),(msg2,diff2)])

            # Remove empty records
            if len(dds['prop_diffs'][shared_node]['type_changes']) == 0:
                del dds['prop_diffs'][shared_node]['type_changes']
            if len(dds['prop_diffs'][shared_node]['enum_diffs']) == 0:
                del dds['prop_diffs'][shared_node]['enum_diffs']
            if list(dds['prop_diffs'][shared_node]) == 0:
                del dds['prop_diffs'][shared_node]
            dds['prop_diffs']

    return dds

def compare_data_dictionaries(commons,outdir='reports',home_dir='.'):
    """
    Writes the summary of commons data dictionary to a file.
    Args:
        commons(dict):
        outdir(str):
        home_dir(str):
    Examples:

    """
    dds = summarize_dd(commons=commons)
    dcs = list(commons.keys())

    os.chdir(home_dir)
    create_output_dir()
    out_name = get_output_name('DD_comparision','txt',commons,outdir)#summary_staging_prod.tsv

    with open(out_name,'w') as report_file:

        for dc in dcs:
            report_file.write("1.0 Dictionary Stats by Data Commons".format())
            report_file.write("\n{} total nodes: {}".format(dc,len(dds[dc].keys())))


        report_file.write("DATA DICTIONARY SUMMARY: {}".format(dcs))

        # Write the node_diffs
        report_file.write("\n\n'{}' Node Differences:\n".format(dc))
        for diff in dds['node_diffs']:
            report_file.write("{}\n".format(diff))

        # Write the prop_diffs
        report_file.write("\n\n'{}' Property Differences:\n".format(dc))
        for node in list(dds['prop_diffs']):
            report_file.write("{}\n\n".format(node))
            for diff in list(dds['prop_diffs'][node]):
                report_file.write("\n\t{}".format(diff))
                report_file.write("\n\t{}".format(dds['prop_diffs'][node][diff]))





def write_summary_report(summary,commons,outdir='reports',home_dir='.'):
    """
    Writes the summary of a commons to a file.
    Args:
        summary(dict):
        commons(dict):
        outdir(str):
        home_dir(str):
    Examples:

    """
    dds = summarize_dd(commons=commons)
    dcs = list(commons.keys())

    os.chdir(home_dir)
    create_output_dir()
    out_name = get_output_name('summary','txt',commons,outdir)#summary_staging_prod.tsv

    x = 1
    for dc in dcs:

        # Write the Data Dictionary summary
        y = 1
        report_file.write("{}.{} '{}' DATA DIRECTORY (location of project TSVs summarized in this section of the report):\n{}".format(x,y,dc,commons[dc]['dir']))
        report_file.write("\n\n{}.{} '{}' DATA DICTIONARY SETTINGS (data dictionary version used for the analysis):\n{}".format(x,y,dc,commons[dc]['dd']['_settings']))

        # Write the project_count, tsv_count, node_count, and prop_count data
        report_file.write("\n\n{}.{} '{}' PROJECT COUNT (the projects with TSV data): {}\n{}\n\n".format(x,y,dc,len(summary[dc]['project_count']),summary[dc]['project_count']))
        y+=1
        report_file.write("\n\n{}.{} '{}' TSV COUNT (the TSV files that were summarized): {}\n{}\n\n".format(x,y,dc,len(summary[dc]['tsv_count']),summary[dc]['tsv_count']))
        y+=1
        report_file.write("\n\n{}.{} '{}' NODE COUNT (nodes in the data model with TSV data): {}\n{}\n".format(x,y,dc,len(summary[dc]['node_count']),summary[dc]['node_count']))
        y+=1
        report_file.write("\n\n{}.{} '{}' PROPERTY COUNT (properties in the data model with TSV data): {}\n{}\n".format(x,y,dc,len(summary[dc]['prop_count']),summary[dc]['prop_count']))
        y+=1

        # Write the Data Summary per Project
        report_file.write("\n\n{}.{} '{}' Data Summary:\n".format(x,y,dc))

        projects = list(summary[dc]['data'].keys())
        for project in projects:
            nodes = list(summary[dc]['data'][project])
            for node in nodes:
                report_file.write("{}".format(node))

    print("Report written to file:\n\t{}\n".format(out_name))


def summarize_tsv_data(
    commons={"staging":{"dir":staging_dir,"dd":sdd},"prod":{"dir":prod_dir,"dd":pdd}},# a dictionary with nickname of each commons as keys, with each value a dictionary of project_tsvs directory ('dir') and the data dictionary ('dd')
    prefix='',# Default gets TSVs from all directories ending in "_tsvs". "prefix" of the project_tsvs directories (e.g., program name of the projects, e.g., "Program_1-Project_2_tsvs"). Result of running the Gen3Expansion.get_project_tsvs() function.
    report=True,# whether to write the data processing results to a txt file.
    outdir='reports',# directory for the output files
    omit_props=['project_id','type','id','submitter_id','case_submitter_id','md5sum','file_name','object_id'],# Properties to omit from being summarized.
    home_dir="."# where to create 'outdir' output files directory
    ):
    """ Adds a summary of TSV data to a dictionary consisting of the following data commons info:
        commons = {"name_of_commons":{"dir":dir_of_project_tsvs, "dd": data_dictionary_of_commons}}
        Can have as many commons.keys() as you like for different directories of TSVs to summarize.
        Args:
            commons(dict):
            prefix(str):
            report(boolean):
            outdir(str):
            omit_props(list):
            home_dir(str):
        Examples:

        s = summarize_tsv_data(
            commons={"staging":{"dir":staging_dir,"dd":sdd},"prod":{"dir":prod_dir,"dd":pdd}},
            prefix='',
            report=True,
            outdir='reports',
            omit_props=['project_id','type','id','submitter_id','case_submitter_id','md5sum','file_name','object_id'],
            home_dir='/Users/christopher/Documents/Notes/BHC/data_qc/')
    """
    summary = {}
    dcs = list(commons.keys()) # get the names of the data commons, e.g., 'staging' and 'prod'
    for dc in dcs:
        summary[dc] = {}

    for dc in dcs: #dc=dcs[0]

        project_count = [] # list of projects in each commons 'dir' directory. Works with output of Gen3Expansion.get_project_tsvs() script.
        tsv_count = [] # list of TSV files across projects in the commons data 'dir'. TSV templates are result of Gen3Submission.export_node() script.
        node_count = [] # list of nodes in the commons data model with non-null data across all projects.
        prop_count = [] # list of properties with non-null data in all nodes across all projects
        data_count = 0 # total count of properties summarized

        os.chdir(commons[dc]['dir'])
        print(os.getcwd())
        dir_pattern = "{}*{}".format(prefix,'tsvs')
        project_dirs = glob.glob(dir_pattern)

        data = {} # all the data for a commons

        for project_dir in project_dirs: # project_dir=project_dirs[0]
            try:
                project_id = re.search(r'^(.+)_tsvs$', project_dir).group(1)
            except:
                print("Couldn't extract the project_id from {}!".format(project_dir))
            data[project_id] = {}
            project_count.append(project_id)

            os.chdir("{}{}".format(commons[dc]['dir'],project_dir))
            print("Changed working directory to:\n\t{}".format(os.getcwd()))

            fpattern = "{}*{}".format(prefix,'.tsv')
            fnames = glob.glob(fpattern)
            print("Found the following {} TSV templates in project {}:\n\n\t{}\n\n".format(len(fnames),project_id,fnames))

            for fname in fnames: # Each node with data in the project is in one TSV file so len(fnames) is the number of nodes in the project with data.
                tsv_count.append(fname)
                node_regex = r"^" + re.escape(project_id) + r"_([a-zA-Z0-9_]+)\.tsv$" #node = re.search(r'^([a-zA-Z0-9_]+)-([a-zA-Z0-9]+)_([a-zA-Z0-9_]+)\.tsv$',fname).group(3)
                try:
                    node = re.search(node_regex, fname, re.IGNORECASE).group(1)
                    print("Extracting data from the '{}' node TSV.".format(node))
                    df = pd.read_csv(fname,sep='\t',header=0,dtype=str)
                    node_count.append(node)
                except Exception as e:
                    print("\nCouldn't get node data for file:\n\t'{}'\n".format(fname))

                data[project_id][node] = {}
                total_records = len(df)

                regex = re.compile(r'^[A-Za-z0-9_]*[^.]$') #drop the links
                props = list(filter(regex.match, list(df))) #properties in this TSV to summarize

                for prop in omit_props: # filter properties (headers) in TSV to remove props we don't want to summarize
                    try:
                        props.remove(prop)
                    except Exception as e:
                        pass #not all nodes will have some props, like md5sum or file_name

                print("Total of {} records in TSV with {} properties.".format(total_records,len(props)))

                data[project_id][node]["properties"] = {} # all the property data stats in a node

                for prop in props: #prop=props[0]
                    data_count+=1
                    data[project_id][node]["properties"][prop] = {}
                    print("'{}', '{}', '{}', '{}'".format(dc,project_id,node,prop))

                    null_count = len(df.loc[df[prop].isnull()])

                    if prop in list(commons[dc]['dd'][node]['properties']): #get the property type, ie, string, enum, integer, number, boolean, array, etc.
                        prop_def = commons[dc]['dd'][node]['properties'][prop]
                        if 'enum' in list(prop_def.keys()):
                            ptype = 'enum'
                        elif 'type' in list(prop_def.keys()):
                            ptype = prop_def['type']
                    else:
                        print("'{}' data dictionary version is mismatched with the TSV data in:\n\t{}\n\nProperty {} not in the data dictionary!".format(dc,dc['dir'],prop))
                        exit(1)

                    data[project_id][node]["properties"][prop]["total_records"] = total_records
                    data[project_id][node]["properties"][prop]["null_count"] = null_count
                    data[project_id][node]["properties"][prop]["property_type"] = ptype

                    if total_records == null_count:
                        print("\t\tSkipping: all null data.".format(prop))
                    else:
                        #print("\t\t'{}'".format(prop))
                        prop_name = "{}.{}".format(node,prop)
                        if not prop_name in prop_count:
                            prop_count.append(prop_name)

                        data[project_id][node]["properties"][prop]["stats"] = {"N":len(df[df[prop].notnull()])}

                        # Get stats for strings
                        if ptype in ['string','enum','boolean']:
                            counts = Counter(df[df[prop].notnull()][prop])
                            df1 = pd.DataFrame.from_dict(counts, orient='index').reset_index()
                            bins = [tuple(x) for x in df1.values]
                            bins.sort(key=itemgetter(1),reverse=True)
                            data[project_id][node]["properties"][prop]["stats"]["bins"] = bins
                            print("\t\tTop 3 bins of {}: {}".format(prop,bins[:3]))

                        # Get stats for numbers
                        elif ptype in ['number','integer']: #prop='concentration'
                            d = list(df[df[prop].notnull()][prop].astype(float))
                            data[project_id][node]["properties"][prop]["stats"]["min"] = min(d)
                            data[project_id][node]["properties"][prop]["stats"]["max"] = max(d)
                            data[project_id][node]["properties"][prop]["stats"]["mean"] = statistics.mean(d)
                            data[project_id][node]["properties"][prop]["stats"]["median"] = statistics.median(d)

                            if len(d) == 1: # If there is only one data point, stdev will error
                                data[project_id][node]["properties"][prop]["stats"]["stdev"] = "NA"
                            else:
                                data[project_id][node]["properties"][prop]["stats"]["stdev"] = statistics.stdev(d)

                            print("\t\t{}".format(data[project_id][node]["properties"][prop]["stats"]))

                        else: # If its not in the list of ptypes, exit. Need to add array handling.
                            print("\t\tUnhandled property type {}".format(prop))
                            exit(1)

        summary[dc]['data'] = data #save data to commons 'data' after looping through all the TSVs from a commons
        summary[dc]['data_count'] = data_count
        summary[dc]['project_count'] = project_count
        summary[dc]['tsv_count'] = tsv_count
        summary[dc]['node_count'] = node_count
        summary[dc]['prop_count'] = prop_count

    if report is True:
        write_summary_report(summary,commons,outdir,homedir)

    os.chdir(home_dir)
    return summary


################################################################################

def write_commons_report(
    summary,#the result of running the 'summarize_tsv_data()' script
    outdir='reports',
    bin_limit=False,# whether to limit the number of bins to summarize for enums, strings, and booleans. If bin_limit=3, only the top 3 bins by their value will be reported.
    create_report=True,# whether to write a TSV report to the outdir
    outdir='reports',
    home_dir="."# the directory name to place output files in
    ):
    """ Write a Data QC Report TSV
    Args:
        summary(dict):
        outdir(str):
        bin_limit(int):
        create_report(bool):
        outdir(str):
        home_dir(str):
    """
    dcs = list(commons.keys())

    total_props = []
    for dc in dcs:
        total_props += summary[dc]['data_count']
    total_props =

    report = pd.DataFrame(index=range(0,total_props),
        columns=['commons','project','node','property','property_type',
                'total_records','null_count','N','bin_number','bins',
                'min','max','mean','median','stdev'])

    i = 0
    for dc in dcs: #dc = dcs[0]
        projects = list(commons[dc]['data'].keys())
        for project in projects: #project=projects[0]
            nodes = list(commons[dc]['data'][project])
            for node in nodes: #node=nodes[0]
                props = list(commons[dc]['data'][project][node]['properties'])
                for prop in props:
                    print("Writing '{}' '{}' '{}' '{}' to report.".format(dc,project,node,prop))
                    data = commons[dc]['data'][project][node]['properties'][prop]
                    #df['column']['row'] = val
                    report['commons'][i] = dc
                    report['project'][i] = project
                    report['node'][i] = node
                    report['property'][i] = prop
                    report['total_records'][i] = data['total_records']
                    report['null_count'][i] = data['null_count']
                    report['property_type'][i] = data['property_type']
                    if 'stats' in data:
                        report['N'][i] = data['stats']['N']
                        if data['property_type'] in ['string','enum','boolean']:
                            prop_bins = data['stats']['bins']
                            bin_number = len(prop_bins.keys())
                            report['bin_number'][i] = bin_number
                            if not bin_limit is False and bin_number > bin_limit:
                                #r['bins'][i] = "Over bin limit ({}).".format(bin_limit)
                                #first_bins = {i: data['stats']['bins'][i] for i in list(data['stats']['bins'])[:2]}
                                # prop_bins = commons[dcs[1]]['data']['mjff-BioFIND']['medication']['properties']['who_drug_name']['stats']
                                # collections.Counter(prop_bins).most_common(10)
                                prop_bins = dict(collections.Counter(prop_bins).most_common(bin_limit))
                                #r['bins'][i] = "First {} bins: {}".format(bin_limit,str(prop_bins))
                                report['bins'][i] = prop_bins
                            else:
                                report['bins'][i] = prop_bins
                        elif data['property_type'] in ['integer','number']:
                            report['min'][i] = data['stats']['min']
                            report['max'][i] = data['stats']['max']
                            report['mean'][i] = data['stats']['mean']
                            report['median'][i] = data['stats']['median']
                            report['stdev'][i] = data['stats']['stdev']
                    i += 1
    if create_report is True:
        os.chdir(home_dir)
        create_output_dir()
        outname = get_output_name('report','tsv',commons,outdir)
        report.to_csv(outname, sep='\t', index=False, encoding='utf-8')

    return report

# r = write_commons_report(s)

# 5) Compare prod/staging stats for each property in each project, considering breaking changes to data model. (e.g., properties that changed nodes/names, "age_at_enrollment" that changed distribution (took lowest value), etc.)



def compare_commons(
    report,#The DataFrame of summary statistics for all the properties per node per project per commons
    outdir='reports',#The directory to write the results DataFrame TSV to
    stats = ['total_records','null_count','N','bins','min','max','mean','median','stdev']#the stats to use for the comparison
    ):
    """ Takes the pandas DataFrame returned from 'write_commons_report'
        where at least 2 data commons are summarized from 'summarize_data_commons'.
    """
    # create prop_ids for comparing project data per property in each node from two data commons
    report['prop_id'] = report['project'] + '_' + report['node'] + '_' + report['property']
    prop_ids = sorted(list(set(report['prop_id'])))
    total = len(prop_ids)

    # initialize results dictionary
    cols = list(report)
    identical = pd.DataFrame(columns=cols)
    different = pd.DataFrame(columns=cols)
    unique = pd.DataFrame(columns=cols)

    i = 0
    for prop_id in prop_ids: # prop_id = prop_ids[0]
        i += 1
        print("({} of {}) Comparing stats for '{}'".format(i,total,prop_id))
        df = report.loc[report['prop_id']==prop_id]
        df['stats'] = df[stats].apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        if len(list(set(df['stats']))) > 1: # if any of the stats for the prop_id is different, each record goes in different df
            different = pd.concat([different,df],ignore_index=True, sort=False)
        else:
            identical = pd.concat([identical,df],ignore_index=True, sort=False)

    # check total
    if len(report) == len(identical) + len(different): #len(report) == len(comparison['identical']) + len(comparison['different'])
        print("Good")
    else:
        print("Doh!")

    if not outdir is False: #if you pass outdir=False as an arg, won't write the TSVs

        diff_name = 'different_'
        for i in range(len(dcs)):
            diff_name += dcs[i]
            if i != len(dcs)-1:
                diff_name += '_'
        diff_name += '.tsv'
        diff_outname = "{}/{}".format(outdir,diff_name)
        different.to_csv(diff_outname, sep='\t', index=False, encoding='utf-8')

        identical_name = 'identical_'
        for i in range(len(dcs)):
            identical_name += dcs[i]
            if i != len(dcs)-1:
                identical_name += '_'
        identical_name += '.tsv'
        identical_outname = "{}/{}".format(outdir,identical_name)
        identical.to_csv(identical_outname, sep='\t', index=False, encoding='utf-8')

        unique_name = 'unique_'
        for i in range(len(dcs)):
            unique_name += dcs[i]
            if i != len(dcs)-1:
                unique_name += '_'
        unique_name += '.tsv'
        unique_outname = "{}/{}".format(outdir,unique_name)
        unique.to_csv(unique_outname, sep='\t', index=False, encoding='utf-8')

    comparison = {"identical":identical,"different":different}
    return comparison


###############################################################################
###############################################################################
# 6) Outlier analysis

def find_outliers(report):
    """ Finds outliers in the data.
    """

    return outliers

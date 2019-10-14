import os, os.path, sys, subprocess, glob, json, datetime, collections
import fnmatch, sys, ntpath, copy, re, operator, requests, statistics
from shutil import copyfile
from pathlib import Path
from collections import Counter
from statistics import mean
from operator import itemgetter
import numpy as np
from numpy import percentile
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
    dcs = list(commons.keys())
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
        if 'type' in prop_def['oneOf'][0]:
            prop_type = prop_def['oneOf'][0]['type']
        elif 'enum' in prop_def['oneOf'][0]:
            prop_type = 'enum'
    elif 'anyOf' in prop_def:
        prop_type = prop_def['anyOf']
    else:
        print("Can't get the property type for {} in {}!".format(shared_prop,dc))
    return prop_type

########################################################################################################################
########################################################################################################################
########################################################################################################################

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
                    if 'oneOf' in commons[dcs[0]]['dd'][shared_node]['properties'][shared_prop]:
                        enums0 = list(commons[dcs[0]]['dd'][shared_node]['properties'][shared_prop]['oneOf'][0]['enum'])
                        enums1 = list(commons[dcs[0]]['dd'][shared_node]['properties'][shared_prop]['oneOf'][0]['enum'])
                    else:
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

########################################################################################################################
#dds=summarize_dd(commons,props_to_remove=['case_submitter_id'],nodes_to_remove=['root','metaschema'])
########################################################################################################################
########################################################################################################################

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
    # unfinished
    return out_name

########################################################################################################################
########################################################################################################################
########################################################################################################################

def write_summary_report(summary,commons,omit_props,outlier_threshold,outdir='reports',home_dir='.'):
    """
    Writes the summary of a commons to a file.
    Args:
        summary(dict):
        commons(dict):
        omit_props(list):
        outlier_threshold(number):
        outdir(str):
        home_dir(str):
    Examples:

    """
    dds = summarize_dd(commons=commons)
    dcs = list(commons.keys())

    os.chdir(home_dir)
    create_output_dir()
    out_name = get_output_name('summary','txt',commons,outdir)#summary_staging_prod.tsv
    with open(out_name,'w') as report_file:
        x = 0
        y = 0
        report_file.write("Section {}.{} QC Pipeline Settings".format(x,y))
        report_file.write("\n\tDate: {}".format(datetime.datetime.now()))
        report_file.write("\n\tProperties omitted from this summary: {}".format(omit_props))
        report_file.write("\n\tThreshold for identifying outliers: mean +/- {} * stdev".format(outlier_threshold))

        for dc in dcs:
            x+=1
            y = 1 # Write the Data Dictionary summary
            report_file.write("\n\nSection {}.{} '{}' DATA DIRECTORY (location of project TSVs summarized in this section of the report):\n{}".format(x,y,dc,commons[dc]['dir']))
            y+=1 # DD Settings
            report_file.write("\n\nSection {}.{} '{}' DATA DICTIONARY SETTINGS (data dictionary version used for the analysis):\n{}".format(x,y,dc,commons[dc]['dd']['_settings']))

            y+=1 # Write the project_count, tsv_count, node_count, and prop_count data
            report_file.write("\n\nSection {}.{} '{}' PROJECT COUNT (the projects with TSV data): {}\n{}\n".format(x,y,dc,len(summary[dc]['project_count']),summary[dc]['project_count']))
            y+=1 # TSV COUNT
            report_file.write("\n\nSection {}.{} '{}' TSV COUNT (the TSV files that were summarized): {}\n{}\n".format(x,y,dc,len(summary[dc]['tsv_count']),summary[dc]['tsv_count']))
            y+=1 # NODE COUNT
            report_file.write("\n\nSection {}.{} '{}' NODE COUNT (nodes in the data model with TSV data): {}\n{}\n".format(x,y,dc,len(summary[dc]['node_count']),summary[dc]['node_count']))
            y+=1 # PROPERTY COUNT
            report_file.write("\n\nSection {}.{} '{}' PROPERTY COUNT (properties in the data model with TSV data): {}\n{}\n".format(x,y,dc,len(summary[dc]['prop_count']),summary[dc]['prop_count']))

            y+=1# Write the data summary for each project
            report_file.write("\n\nSection {}.{} '{}' Data Summary:\n".format(x,y,dc))
            projects = list(summary[dc]['data'].keys())
            for project in projects:
                nodes = list(summary[dc]['data'][project])
                for node in nodes:
                    null_props = []
                    outliers = []
                    props = list(summary[dc]['data'][project][node]['properties'])
                    for prop in props:
                        total_records = summary[dc]['data'][project][node]['properties'][prop]['total_records']
                        null_count = summary[dc]['data'][project][node]['properties'][prop]['null_count']
                        if total_records == null_count: #write these together at the end of each node, no need to print stats for null data.
                            null_props.append(prop)
                        else:
                            report_file.write("\n['{}','{}','{}','{}']: ".format(dc, project, node, prop))
                            prop_data = summary[dc]['data'][project][node]['properties'][prop]
                            report_file.write("Total records: {}, null: {}".format(prop_data['total_records'],prop_data['null_count']))
                            stats = list(summary[dc]['data'][project][node]['properties'][prop]['stats'])
                            for stat in stats:
                                stat_data = summary[dc]['data'][project][node]['properties'][prop]['stats'][stat]
                                report_file.write(", {}: {}".format(stat,stat_data))
                    if len(null_props) > 0:
                        report_file.write("\n['{}','{}','{}']: {} properties in TSV with all null data: {}\n".format(dc, project, node, len(null_props), null_props))
    print("Report written to file:\n\t{}\n".format(out_name))
    return out_name

########################################################################################################################
# write_summary_report(summary,commons,omit_props,outlier_threshold,outdir='reports',home_dir='/Users/christopher/Documents/Notes/BHC/data_qc/')


########################################################################################################################
########################################################################################################################

def summarize_tsvs(commons,prefix='',report=True,outlier_threshold=3,omit_props=['project_id','type','id','submitter_id','case_submitter_id','md5sum','file_name','object_id'],home_dir=".",outdir='reports'):
    """
    Returns a nested dictionary of summarized TSV data per commons, project, node, and property.
    For each property, the total number of records in the project with and without data is returned.
    Bins and the number of unique bins are returned for string, enumeration and boolean properties.
    The mean, median, min, max, and stdev are returned for integers and numbers.
    Outliers in numeric data are identified using "+/- stdev". The cut-off for outlier identification can be changed by raising or lowering the outlier_threshold (default=3).
    Args:
        commons(dict): keys are abbreviated data commons names(str), with each value a dict with project_tsvs directory ('dir') the corresponding data dictionary ('dd')
        prefix(str): Default gets TSVs from all directories ending in "_tsvs". "prefix" of the project_tsvs directories (e.g., program name of the projects: "Program_1-Project_2_tsvs"). Result of running the Gen3Expansion.get_project_tsvs() function.
        report(boolean): Whether to write the data processing results to a txt file.
        outlier_threshold(number): The upper/lower threshold for identifying outliers in numeric data is the standard deviation multiplied by this number.
        omit_props(list): Properties to omit from being summarized. It doesn't make sense to summarize certain properties, e.g., those with all unique values. May want to omit: ['sample_id','specimen_number','current_medical_condition_name','medical_condition_name','imaging_results','medication_name'].
        home_dir(str):where to create 'outdir' output files directory
        outdir(str): A directory for the output files.
    Examples:
        s = summarize_tsv_data(
            commons={"staging":{"dir":staging_dir,"dd":sdd},"prod":{"dir":prod_dir,"dd":pdd}},
            prefix='',
            report=True,
            outdir='reports',
            omit_props=['project_id','type','id','submitter_id','case_submitter_id','md5sum','file_name','object_id'],
            home_dir='/Users/christopher/Documents/Notes/BHC/data_qc/',
            outlier_threshold=3)
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
                try: # extract the node name from the filename
                    node = re.search(node_regex, fname, re.IGNORECASE).group(1)
                    df = pd.read_csv(fname,sep='\t',header=0,dtype=str)
                except Exception as e:
                    print("\nCouldn't find a '{}' TSV file:\n\t'{}'\n".format(node,e))

                total_records = len(df)
                if total_records > 0:
                    node_count.append(node)
                    print("\tExtracting '{}' TSV data.".format(node))
                    data[project_id][node] = {}

                    regex = re.compile(r'^[A-Za-z0-9_]*[^.]$') #drop the links, e.g., cases.submitter_id or diagnoses.id
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

                        ptype = get_prop_type(node,prop,dc,commons)
                        data[project_id][node]["properties"][prop]["total_records"] = total_records
                        data[project_id][node]["properties"][prop]["null_count"] = null_count
                        data[project_id][node]["properties"][prop]["property_type"] = ptype

                        if total_records == null_count:
                            print("\t\tAll null data for '{}' in this TSV.".format(prop))
                        else:
                            #print("\t\t'{}'".format(prop))
                            prop_name = "{}.{}".format(node,prop)
                            if not prop_name in prop_count:
                                prop_count.append(prop_name)

                            data[project_id][node]["properties"][prop]["stats"] = {"N":len(df[df[prop].notnull()])}

                            # Get stats for strings
                            if ptype in ['string','enum','boolean','date']:
                                counts = Counter(df[df[prop].notnull()][prop])
                                df1 = pd.DataFrame.from_dict(counts, orient='index').reset_index()
                                bins = [tuple(x) for x in df1.values]
                                # bins = [('a',10),('b',10),('c',5),('d',5),('e',1),('f',1)]
                                # bins2 = [('e',10),('f',10),('d',5),('c',5),('b',1),('a',1)]
                                # sorted(bins,key=lambda x: (x[0])) #sort bins by name
                                # sorted(bins,key=lambda x: (x[1]),reverse=True) # sort bins by value
                                # bins = sorted(bins,key=lambda x: (x[1], x[0]),reverse=True) # this sorts both value and name in reverse.
                                bins = sorted(sorted(bins,key=lambda x: (x[0])),key=lambda x: (x[1]),reverse=True) # sort first by name, then by value. This way, names with same value are in same order.
                                data[project_id][node]["properties"][prop]["stats"]["bins"] = bins
                                data[project_id][node]["properties"][prop]["stats"]["bin_number"] = len(bins)
                                print("\t\tTop 3 bins out of {} for {}: {}".format(len(bins),prop,bins[:3]))

                            # Get stats for numbers
                            elif ptype in ['number','integer']: #prop='concentration'
                                d = list(df[df[prop].notnull()][prop].astype(float))
                                mn = statistics.mean(d)
                                data[project_id][node]["properties"][prop]["stats"]["mean"] = mn
                                md = statistics.median(d)
                                data[project_id][node]["properties"][prop]["stats"]["median"] = md
                                data[project_id][node]["properties"][prop]["stats"]["min"] = min(d)
                                data[project_id][node]["properties"][prop]["stats"]["max"] = max(d)
                                if len(d) == 1: # If there is only one data point, stdev will error
                                    data[project_id][node]["properties"][prop]["stats"]["stdev"] = "NA"
                                    data[project_id][node]["properties"][prop]["stats"]["outliers"] = []
                                else:
                                    std = statistics.stdev(d)
                                    data[project_id][node]["properties"][prop]["stats"]["stdev"] = std
                                    # Get outliers by mean +/- outlier_threshold * stdev
                                    cutoff = std * outlier_threshold # three times the standard deviation is default
                                    lower, upper = mn - cutoff, mn + cutoff # cut-offs for outliers is 3 times the stdev below and above the mean
                                    outliers = sorted(list(set([x for x in d if x < lower or x > upper])))
                                    data[project_id][node]["properties"][prop]["stats"]["outliers"] = outliers

                                print("\t\t{}".format(data[project_id][node]["properties"][prop]["stats"]))

                            else: # If its not in the list of ptypes, exit. Need to add array handling.
                                print("\t\tUnhandled property type {}".format(prop))
                                exit(1)
                else:
                    print("\t{} records in '{}' TSV. No data to summarize.".format(total_records,node))

        summary[dc]['data'] = data #save data to commons 'data' after looping through all the TSVs from a commons
        summary[dc]['data_count'] = data_count
        summary[dc]['project_count'] = project_count
        summary[dc]['tsv_count'] = tsv_count
        summary[dc]['node_count'] = node_count
        summary[dc]['prop_count'] = prop_count

    if report is True:
        write_summary_report(summary,commons,omit_props,outlier_threshold,outdir,home_dir)

    os.chdir(home_dir)
    return summary

########################################################################################################################
# summary = summarize_tsvs(commons={"staging":{"dir":staging_dir,"dd":sdd},"prod":{"dir":prod_dir,"dd":pdd}},prefix='',report=True,outdir='reports',omit_props=['project_id','type','id','submitter_id','case_submitter_id','md5sum','file_name','object_id'],home_dir='/Users/christopher/Documents/Notes/BHC/data_qc/',outlier_threshold=3)

########################################################################################################################
########################################################################################################################

def write_commons_report(summary,commons,bin_limit=False,create_report=True,report_null=True,outdir='reports',home_dir="."):
    """ Converts the summarize_tsvs() dictionary into a pandas DataFrame and writes it to a file.
    Args:
        summary(dict): the dict returned from running 'summarize_tsvs()' script.
        commons(dict): the dictionary containing the data commons TSV dirs and data dictionaries.
        bin_limit(int): limits the number of bins written to the report for enums, strings, and booleans. If bin_limit=3, only the largest 3 bins will be reported.
        create_report(bool): whether to write a TSV report to the outdir
        report_null(bool): if False, properties in TSVs with entirely null data will be excluded from the report.
        outdir(str): Directory to write the report file to.
        home_dir(str): the directory where project TSV folders and 'outdir' are.
    """
    dcs = list(summary.keys())

    total_props = 0
    for dc in dcs:
        total_props += summary[dc]['data_count']

    report = pd.DataFrame(index=range(0,total_props),
        columns=['commons','project','node','property','property_type',
                'total_records','null_count','all_null',
                'N','min','max','median','mean','stdev','outliers',
                'bin_number','bins'])

    i = 0
    for dc in dcs: #dc = dcs[0]
        projects = list(summary[dc]['data'].keys())
        for project in projects: #project=projects[0]
            nodes = list(summary[dc]['data'][project])
            for node in nodes: #node=nodes[0]
                props = list(summary[dc]['data'][project][node]['properties'])
                for prop in props:
                    print("Writing '{}' '{}' '{}' '{}' to report.".format(dc,project,node,prop))
                    data = summary[dc]['data'][project][node]['properties'][prop]
                    total_records = data['total_records']
                    null_count = data['null_count']

                    if total_records > null_count or report_null is True:
                        report['total_records'][i] = total_records
                        report['null_count'][i] = null_count
                        report['commons'][i] = dc
                        report['project'][i] = project
                        report['node'][i] = node
                        report['property'][i] = prop
                        report['property_type'][i] = data['property_type']
                        if 'stats' in data:
                            report['N'][i] = data['stats']['N']
                            if data['property_type'] in ['string','enum','boolean']:
                                prop_bins = data['stats']['bins']
                                bin_number = len(prop_bins)
                                report['bin_number'][i] = bin_number
                                if not bin_limit is False and bin_number > bin_limit:
                                    report['bins'][i] = prop_bins[:bin_limit]
                                else:
                                    report['bins'][i] = prop_bins
                            elif data['property_type'] in ['integer','number']:
                                report['min'][i] = data['stats']['min']
                                report['max'][i] = data['stats']['max']
                                report['mean'][i] = data['stats']['mean']
                                report['median'][i] = data['stats']['median']
                                report['stdev'][i] = data['stats']['stdev']
                                report['outliers'][i] = data['stats']['outliers']
                        if total_records == null_count:
                            report['all_null'][i] = True
                        else:
                            report['all_null'][i] = False
                        i += 1
    report['prop_id'] = report['project'] + '.' + report['node'] + '.' + report['property']

    if create_report is True:
        os.chdir(home_dir)
        create_output_dir()
        outname = get_output_name('report','tsv',summary,outdir)
        report.to_csv(outname, sep='\t', index=False, encoding='utf-8')
        print("\nReport written to file:\n\t{}".format(outname))

    return report


########################################################################################################################
# report = write_commons_report(summary,bin_limit=False,create_report=True,outdir='reports',home_dir='/Users/christopher/Documents/Notes/BHC/data_qc/')


########################################################################################################################
########################################################################################################################

def compare_commons(report,commons,stats = ['total_records','null_count','N','min','max','mean','median','stdev','bin_number','bins'],create_report=True,home_dir='.',outdir='reports'):
    """ Takes the pandas DataFrame returned from 'write_commons_report'
        where at least 2 data commons are summarized from 'summarize_data_commons'.
    Args:
        report(dataframe): a pandas dataframe generated from a summary of TSV data; obtained by running write_summary_report() on the result of summarize_tsv_data()
        commons(dict): The data commons to summarize. Keys should be the names of the data commons.
        stats(list): the list of statistics to compare between data commons for each node/property combination
        outdir(str): directory within home_dir to save output files to.
        home_dir(str): directory within which to create the outdir directory for reports.
        create_report(boolean): If True, reports are written to files in the outdir.
    """
    # This script only compares the first two data commons in a report
    dcs = list(set(list(report['commons'])))[:2]
    report = report.loc[report['commons'].isin(dcs)]

    # create prop_ids for comparing project data per property in each node from two data commons
    prop_ids = sorted(list(set(report['prop_id'])))
    total = len(prop_ids)

    # initialize results dictionary
    cols = list(report)
    #all_data = pd.DataFrame(index=range(0,total),columns=cols)
    unclassified = pd.DataFrame(columns=cols)
    identical = pd.DataFrame(columns=cols)
    different = pd.DataFrame(columns=cols)
    unique = pd.DataFrame(columns=cols)

    dcs_stats = []
    for stat in stats:
        for dc in dcs:
            dcs_stats.append(dc + '_' + stat)
    comparison_cols = ['project_id','node','property','prop_id'] + dcs_stats
    comparison = pd.DataFrame(columns=comparison_cols, index=prop_ids)

    i = 1 # to track the progress of the script in print statement below
    for prop_id in prop_ids: # prop_id = prop_ids[0]

        print("({} of {} prop_ids) Comparing stats for '{}'".format(i,total,prop_id))
        i += 1

        project_id,node,prop = prop_id.split('.')
        comparison['project_id'][prop_id] = project_id
        comparison['node'][prop_id] = node
        comparison['property'][prop_id] = prop
        comparison['prop_id'][prop_id] = prop_id

        df = report.loc[report['prop_id']==prop_id].reset_index(drop=True)

        if len(df) == 1: # if only one instance of the project-node-property in the report, put in unique df
            unique = pd.concat([unique,df],ignore_index=True, sort=False)
            #print("\tUnique".format(prop_id))

        elif len(df) == 2: # do the comparison and save results to comparison, then also to different or identical
            same = []
            for stat in stats: # first, check whether any of the stats are different bw commons
                if df[stat][0] != df[stat][1]: # Note: if both values are "NaN" this is True; because NaN != NaN
                    if list(df[stat].isna())[0] is True and list(df[stat].isna())[1] is True:# if stats are both "NaN", data are identical
                        same.append(True)
                    else: # if stats are different AND both values aren't "NaN", data are different
                        same.append(False)
                else: # if stat0 is stat1, data are identical
                    same.append(True)

            if False in same: # if any of the stats are different bw commons, add to different df and comparison df
                different = pd.concat([different,df],ignore_index=True, sort=False)
                for stat in stats:
                    col0 = dcs[0]+'_'+stat
                    col1 = dcs[1]+'_'+stat
                    comparison[col0][prop_id] = df[stat][0]
                    comparison[col1][prop_id] = df[stat][1]
            else:
                identical = pd.concat([identical,df],ignore_index=True, sort=False)

        else:
            print("\n\nThe number of instances of this project-node-property '{}' is not 1 or 2!\n{}\n\n".format(prop_id,df))
            unclassified = pd.concat([unclassified,df],ignore_index=True, sort=False)

        # drop all prop_ids that don't have different data (comparison columns only get filled if stats are different)
    comparison = comparison[list(comparison)].dropna(thresh=5) # only the project,node,property, and prop_id columns have non-null values if stats aren't different, so thresh=5 gets rid of prop_ids with unique/identical data

    # check total
    if len(report) == len(identical) + len(different) + len(unique): #len(report) == len(comparison['identical']) + len(comparison['different'])
        print("All {} properties in the report (instances of {} unique prop_ids) were classified as having unique, identical or different data between data commons: {}.".format(len(report),total,dcs))
    else:
        print("Some properties in the report were not classified!")

    if create_report is True:

        os.chdir(home_dir)
        create_output_dir(outdir)

        comp_name = get_output_name("comparison","tsv",commons,outdir='reports')
        comparison.to_csv(comp_name, sep='\t', index=False, encoding='utf-8')

        diff_name = get_output_name("different","tsv",commons,outdir='reports')
        different.to_csv(diff_name, sep='\t', index=False, encoding='utf-8')

        identical_name = get_output_name("identical","tsv",commons,outdir='reports')
        identical.to_csv(identical_name, sep='\t', index=False, encoding='utf-8')

        unique_name = get_output_name("unique","tsv",commons,outdir='reports')
        unique.to_csv(unique_name, sep='\t', index=False, encoding='utf-8')

    dfs = {"comparison":comparison,
        "different":different,
        "identical":identical,
        "unclassified":unclassified,
        "unique":unique}

    return dfs

########################################################################################################################
########################################################################################################################
#c = compare_commons_reports(report,stats = ['total_records','null_count','N','min','max','mean','median','stdev','bin_number','bins'],outdir='reports',home_dir='/Users/christopher/Documents/Notes/BHC/data_qc/',create_report=True)

########################################################################################################################

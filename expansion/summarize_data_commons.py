"""
## Setup interactive python environment (iPython or Jupyter Notebook)
#
##############################################################################
## Setup using pip:
#
# import gen3
# from gen3.auth import Gen3Auth
# from gen3.submission import Gen3Submission
#
# Download gen3sdk scripts from GitHub
# !wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/auth.py
# !wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
# !wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
# %run ./auth.py
# %run ./submission.py
# auth = Gen3Auth(api, refresh_file=cred)
# sub = Gen3Submission(api, auth)
# %run ./expansion.py
# exp = Gen3Expansion(api, auth)
#

##############################################################################
# Setup using local SDK files cloned from GitHub:
# in git_dir, do "git clone git@github.com:cgmeyer/gen3sdk-python.git"

profile = 'acct'
api = 'https://acct.bionimbus.org/'
creds = '/Users/christopher/Downloads/acct-credentials.json'

import pandas as pd
import sys

git_dir='/Users/christopher/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from expansion.expansion import Gen3Expansion
from migration.migration import Gen3Migration
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
auth = Gen3Auth(api, refresh_file=creds)
sub = Gen3Submission(api, auth) # Initialize an instance this class, using your creds in 'auth'

# run my local working copy
%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py # Some additional functions in Gen3Expansion class
exp = Gen3Expansion(api, auth) # Initialize an instance, using its functions like exp.get_project_tsvs()

"""

# import os, os.path, sys, subprocess, glob, json, datetime, collections
# import fnmatch, sys, ntpath, copy, re, operator, requests, statistics
# from shutil import copyfile
# from pathlib import Path
# from collections import Counter
# from statistics import mean
# from operator import itemgetter
# import numpy as np
# from numpy import percentile
# from scipy import stats
# import pandas as pd
# from pandas.io.json import json_normalize
# pd.options.mode.chained_assignment = None # turn off pandas chained assignment warning
# import sys
# if sys.version_info[0] < 3:
#     from StringIO import StringIO
# else:
#     from io import StringIO


import statistics

def t(var):
    vtype = type(var)
    print(vtype)
    if vtype in [dict,list]:
        print("{}".format(list(var)))
    if vtype in [str,int,float]:
        print("{}".format(var))


def get_output_name(name,extension='tsv',outdir='data_summary_reports'):
    names = name.split('/')
    names = [name for name in names if name != '']
    basename = names[-1]
    outname = "data_summary_{}".format(basename)
    outname += '.{}'.format(extension)
    outname = "{}/{}".format(outdir,outname)# data_summary_reports/summary_staging_prod.tsv
    return outname


def create_output_dir(outdir='data_summary_reports'):
    cmd = ['mkdir','-p',outdir]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
    except Exception as e:
        output = e.output.decode('UTF-8')
        print("ERROR:" + output)
    return outdir


def list_links(link_list,dd):
    """ return a list of indiv link names.
    """
    link_names = []
    for link in link_list:
        if 'subgroup' in link:
            sublinks = list(link['subgroup'])
            for sublink in sublinks:
                link_names.append(sublink['name'])
        else:
            link_names.append(link['name'])
    return link_names


def get_prop_type(node,prop,dd):
    prop_def = dd[node]['properties'][prop]
    if 'type' in prop_def:
        prop_type = prop_def['type']
        if 'null' in prop_type:
            prop_type = [prop for prop in prop_type if prop != 'null'][0]
    elif 'enum' in prop_def:
        prop_type = 'enum'
    elif 'oneOf' in prop_def:
        if 'type' in prop_def['oneOf'][0]:
            prop_type = prop_def['oneOf'][0]['type']
        elif 'enum' in prop_def['oneOf'][0]:
            prop_type = 'enum'
    elif 'anyOf' in prop_def:
        if isinstance(prop_def['anyOf'],list):
            prop_type = [x['type'] for x in prop_def['anyOf'] if 'items' in x][0]
        else:
            prop_type = prop_def['anyOf']
    else:
        print("Can't get the property type for {}!".format(shared_prop))
    return prop_type


def summarize_dd(api,props_to_remove=['case_submitter_id'],nodes_to_remove=['root','metaschema']):
    """ Return a dict with nodes and list of properties in each node.

        Args:
            api(str): the data commons API endpoint, e.g., api = 'https://acct.bionimbus.org/'
    """
    dd = self.sub.get_dictionary_all()
    nodes = []
    node_regex = re.compile(r'^[^_][A-Za-z0-9_]+$')# don't match _terms,_settings,_definitions, etc.)
    nodes = list(filter(node_regex.search, list(dd)))

    dds = {}
    for node in nodes:
        dds[node] = []
        props = list(dd[node]['properties'])
        for prop in props:
            dds[node].append(prop)

    return dds



########################################################################################################################
########################################################################################################################

def summarize_tsvs(tsv_dir,dd,prefix='',outlier_threshold=3,omit_props=['project_id','type','id','submitter_id','case_submitter_id','case_ids','visit_id','sample_id','md5sum','file_name','object_id'],omit_nodes=['metaschema','root','program','project','data_release'],outdir='data_summary_reports'):
    """
    Returns a nested dictionary of summarized TSV data per project, node, and property.
    For each property in each project, the total, non-null and null counts are returned.
    For string, enumeration and boolean properties, bins and the number of unique bins are returned.
    For integers and numbers, the mean, median, min, max, and stdev are returned.
    Outliers in numeric data are identified using "+/- stdev". The cut-off for outlier identification can be changed by raising or lowering the outlier_threshold (default=3).

    Args:
        tsv_dir(str): project_tsvs directory
        dd(dict): data dictionary of the commons result of func Gen3Submission.get_dictionary_all()
        prefix(str): Default gets TSVs from all directories ending in "_tsvs". "prefix" of the project_tsvs directories (e.g., program name of the projects: "Program_1-Project_2_tsvs"). Result of running the Gen3Expansion.get_project_tsvs() function.
        outlier_threshold(number): The upper/lower threshold for identifying outliers in numeric data is the standard deviation multiplied by this number.
        omit_props(list): Properties to omit from being summarized. It doesn't make sense to summarize certain properties, e.g., those with all unique values. May want to omit: ['sample_id','specimen_number','current_medical_condition_name','medical_condition_name','imaging_results','medication_name'].
        omit_nodes(list): Nodes in the data dictionary to omit from being summarized, e.g., program, project, data_release, root and metaschema.
        outdir(str): A directory for the output files.

    Examples:
        s = summarize_tsvs(tsv_dir='/Users/christopher/Documents/Notes/ACCT/tsvs/project_tsvs_04222020/',
            dd=dd,
            prefix='',
            outdir='data_summary_reports',
            omit_props=['project_id','type','id','submitter_id','case_ids','md5sum','file_name','object_id'],
            omit_nodes=['metaschema','root','program','project','data_release'],
            outlier_threshold=3)
    """
    summary = {}

    os.chdir(tsv_dir)
    print(os.getcwd())
    dir_pattern = "{}*{}".format(prefix,'tsvs')
    project_dirs = glob.glob(dir_pattern)

    data,nn_nodes,nn_props,null_nodes,null_props,all_prop_ids = {},[],[],[],[],[]

    for project_dir in project_dirs: # project_dir=project_dirs[0]

        try:
            project_id = re.search(r'^(.+)_tsvs$', project_dir).group(1)
        except:
            print("Couldn't extract the project_id from {}!".format(project_dir))

        os.chdir("{}/{}".format(tsv_dir,project_dir))
        print("\tSummarizing data in project '{}'".format(project_id))

        fpattern = "{}*{}".format(prefix,'.tsv')
        fnames = glob.glob(fpattern)
        print("\t\tFound the following {} TSVs: {}".format(len(fnames),fnames))

        data[project_id] = {}
        data[project_id]['nodes'] = {} # currently 'nodes' is the only key in a project's dictionary, but leaving it for now in case I want to add other project specific stats

        for fname in fnames: # Each node with data in the project is in one TSV file so len(fnames) is the number of nodes in the project with data.

            node_regex = r"^" + re.escape(project_id) + r"_([a-zA-Z0-9_]+)\.tsv$" #node = re.search(r'^([a-zA-Z0-9_]+)-([a-zA-Z0-9]+)_([a-zA-Z0-9_]+)\.tsv$',fname).group(3)

            try: # extract the node name from the filename
                node = re.search(node_regex, fname, re.IGNORECASE).group(1)
                df = pd.read_csv(fname, sep='\t', header=0, dtype=str)
            except Exception as e:
                print("\nCouldn't find a '{}' TSV file:\n\t'{}'\n".format(node,e))

            if df.empty:
                print("\t\t'{}' TSV is empty. No data to summarize.".format(node))

            else:
                nn_nodes.append(node)
                prop_regex = re.compile(r'^[A-Za-z0-9_]*[^.]$') #drop the links, e.g., cases.submitter_id or diagnoses.id (matches all properties with no ".")
                props = list(filter(prop_regex.match, list(df))) #properties in this TSV to summarize
                props = [prop for prop in props if prop not in omit_props]

                print("\t\tTotal of {} records in '{}' TSV with {} properties.".format(len(df),node,len(props)))

                data[project_id]['nodes'][node] = {}

                for prop in props: #prop=props[0]

                    data[project_id]['nodes'][node][prop] = {}

                    # because of sheepdog bug, need to inclue "None" in "null" (:facepalm:) https://ctds-planx.atlassian.net/browse/PXP-5663
                    df.at[df[prop] == 'None',prop] = np.nan
                    null = df.loc[df[prop].isnull()]
                    nn = df.loc[df[prop].notnull()]

                    ptype = get_prop_type(node,prop,dd)

                    data[project_id]['nodes'][node][prop]["N"] = len(df)
                    data[project_id]['nodes'][node][prop]["null"] = len(null)
                    data[project_id]['nodes'][node][prop]["nn"] = len(nn)
                    data[project_id]['nodes'][node][prop]["type"] = ptype

                    prop_name = "{}.{}".format(node,prop)
                    prop_id = "{}.{}".format(project_id,prop_name)

                    if nn.empty:
                        #print("\t\tAll null data for '{}' in this TSV.".format(prop))
                        null_props.append(prop_name)

                    else:
                        nn_props.append(prop_name)
                        all_prop_ids.append(prop_id)

                        # Get stats for strings
                        if ptype in ['string','enum','boolean','date','array']:

                            counts = Counter(nn[prop])
                            df1 = pd.DataFrame.from_dict(counts, orient='index').reset_index()
                            bins = [tuple(x) for x in df1.values]
                            bins = sorted(sorted(bins,key=lambda x: (x[0])),key=lambda x: (x[1]),reverse=True) # sort first by name, then by value. This way, names with same value are in same order.

                            data[project_id]['nodes'][node][prop]['bins'] = bins
                            data[project_id]['nodes'][node][prop]['bin_number'] = len(bins)
                            print("\t\t'{}.{}.{}': {}".format(project_id,node,prop,data[project_id]['nodes'][node][prop]))

                        # Get stats for numbers
                        elif ptype in ['number','integer']: #prop='concentration'

                            # make a list of the data values as floats (converted from strings)
                            d = list(nn[prop].astype(float))

                            # calculate summary stats using the float list d
                            mean = statistics.mean(d)
                            median = statistics.median(d)
                            minimum = min(d)
                            maximum = max(d)

                            if len(d) == 1: # if only one value, no stdev and no outliers
                                std = "NA"
                                outliers = []
                            else:
                                std = statistics.stdev(d)
                                # Get outliers by mean +/- outlier_threshold * stdev
                                cutoff = std * outlier_threshold # three times the standard deviation is default
                                lower, upper = mean - cutoff, mean + cutoff # cut-offs for outliers is 3 times the stdev below and above the mean
                                outliers = sorted(list(set([x for x in d if x < lower or x > upper])))

                            # if property type is 'integer', change min, max, median to int type
                            if ptype == 'integer':
                                median = int(median) # median
                                minimum = int(minimum) # min
                                maximum = int(maximum) # max
                                outliers = [int(i) for i in outliers] # convert outliers from float to int

                            data[project_id]['nodes'][node][prop]['stdev'] = std
                            data[project_id]['nodes'][node][prop]['mean'] = mean
                            data[project_id]['nodes'][node][prop]['median'] = median
                            data[project_id]['nodes'][node][prop]['min'] = minimum
                            data[project_id]['nodes'][node][prop]['max'] = maximum
                            data[project_id]['nodes'][node][prop]['outliers'] = outliers

                            print("\t\t'{}.{}.{}': {}".format(project_id,node,prop,data[project_id]['nodes'][node][prop]))

                        else: # If its not in the list of ptypes, exit. Need to add array handling.
                            print("\t\tUnhandled property type {}: {}".format(prop,ptype))
                            exit()

    summary['data'] = data
    summary['all_prop_ids'] = all_prop_ids

    # summarize all properties
    nn_props = sorted(list(set(nn_props)))
    summary['nn_props'] = nn_props

    null_props = [prop for prop in null_props if prop not in nn_props]
    summary['null_props'] = sorted(list(set(null_props)))

    # summarize all nodes
    nn_nodes = sorted(list(set(nn_nodes)))
    summary['nn_nodes'] = nn_nodes

    dd_regex = re.compile(r'[^_][A-Za-z0-9_]+')
    dd_nodes = list(filter(dd_regex.match, list(dd)))
    dd_nodes = [node for node in dd_nodes if node not in omit_nodes]
    null_nodes = [node for node in dd_nodes if node not in nn_nodes]

    summary['null_nodes'] = null_nodes

    os.chdir(tsv_dir)
    return summary


########################################################################################################################

# Usage example:

# tsv_dir = '/Users/christopher/Documents/Notes/ACCT/tsvs/project_tsvs_04222020/'
# dd = sub.get_dictionary_all()
#
# tsv_dir = '/Users/christopher/Documents/Notes/BHC/data_qc/dm2.2_QC/prep_tsvs_04152020'
# prep_dd = prep_sub.get_dictionary_all()
#s = summarize_tsvs(tsv_dir,dd)


########################################################################################################################
########################################################################################################################
########################################################################################################################

def write_commons_report(summary,tsv_dir,outdir='.',bin_limit=False,create_report=True,report_null=True):
    """ Converts the summarize_tsvs() dictionary into a pandas DataFrame and writes it to a file.
    Args:
        summary(dict): the dict returned from running 'summarize_tsvs()' script.
        tsv_dir(str): the directory where project TSV folders and 'outdir' are.
        outdir(str): Directory to write the report file to.
        bin_limit(int): limits the number of bins written to the report for enums, strings, and booleans. If bin_limit=3, only the largest 3 bins will be reported.
        create_report(bool): whether to write a TSV report to the outdir
        report_null(bool): if False, properties in TSVs with entirely null data will be excluded from the report.

    Example:
        r = write_commons_report(summary = s,
            tsv_dir = '/Users/christopher/Documents/Notes/BHC/data_migration/v2.2/prod_tsvs_04112020',
            outdir = '.'
            bin_limit = False,
            create_report = True,
            report_null = True)

    """

    # count the total number of properties in "summary" to write to report
    total_props = len(summary['all_prop_ids'])

    report = pd.DataFrame(index=range(0,total_props),
        columns=['prop_id','project_id','node','property','property_type',
                'N','nn','null','all_null',
                'min','max','median','mean','stdev','outliers',
                'bin_number','bins'])

    i = 0

    project_ids = list(s['data'])

    for project_id in project_ids: #project_id=project_ids[0]
        print("Reporting '{}' data:".format(project_id))
        nodes = list(summary['data'][project_id]['nodes'])
        for node in nodes: #node=nodes[0]
            props = list(summary['data'][project_id]['nodes'][node])
            for prop in props: #prop=props[0]

                prop_id = "{}.{}.{}".format(project_id,node,prop)
                stats = summary['data'][project_id]['nodes'][node][prop]

                if stats['null'] == stats['N'] and report_null is False:
                    print("\t'{}' not written to report. All records are null and 'report_null' option is set to 'False'!".format(prop_id))

                else:

                    report['prop_id'][i] = prop_id
                    report['project_id'][i] = project_id
                    report['node'][i] = node
                    report['property'][i] = prop

                    report['N'][i] = stats['N']
                    report['nn'][i] = stats['nn']
                    report['null'][i] = stats['null']
                    report['property_type'][i] = stats['type']

                    if stats['nn'] == 0:
                        report['all_null'][i] = True
                    else:
                        report['all_null'][i] = False

                        if stats['type'] in ['string','enum','boolean','array']:
                            report['bin_number'][i] = stats['bin_number']

                            bins = stats['bins']
                            if not bin_limit is False and stats['bin_number'] > bin_limit:
                                report['bins'][i] = stats['bins'][:bin_limit]
                            else:
                                report['bins'][i] = stats['bins']

                        elif stats['type'] in ['integer','number']:
                            report['min'][i] = stats['min']
                            report['max'][i] = stats['max']
                            report['mean'][i] = stats['mean']
                            report['median'][i] = stats['median']
                            report['stdev'][i] = stats['stdev']
                            report['outliers'][i] = stats['outliers']

                        else:
                            print("Unhandled property: '{}' of type '{}'!".format(prop_id,stats['type']))

                    print("\t'{}' written to report ({} total, {} null, {} non-null).".format(prop_id,stats['N'],stats['null'],stats['nn']))
                    i += 1

    if create_report is True:
        os.chdir(tsv_dir)
        create_output_dir(outdir='data_summary_reports')
        outname = get_output_name(name=tsv_dir,extension='tsv',outdir='data_summary_reports')
        report.to_csv(outname, sep='\t', index=False, encoding='utf-8')
        print("\nReport written to file:\n\t{}".format(outname))

    return report


########################################################################################################################

tsv_dir = '/Users/christopher/Documents/Notes/ACCT/tsvs/project_tsvs_04222020/'
report = write_commons_report(summary=s, outdir='.', tsv_dir=tsv_dir)


########################################################################################################################
########################################################################################################################

def compare_commons(report,commons,stats = ['total_records','null_count','N','min','max','mean','median','stdev','bin_number','bins'],create_report=True,home_dir='.',outdir='data_summary_reports'):
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
                for stat in stats: #stat=stats[0]

                    col0 = dcs[0]+'_'+stat #column name for first commons
                    col1 = dcs[1]+'_'+stat #column name for second commons
                    comparison[col0][prop_id] = df.loc[df['commons']==dcs[0]].iloc[0][stat]
                    comparison[col1][prop_id] = df.loc[df['commons']==dcs[1]].iloc[0][stat]

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

        comp_name = get_output_name("comparison","tsv",commons,outdir=outdir)
        comparison.to_csv(comp_name, sep='\t', index=False, encoding='utf-8')

        diff_name = get_output_name("different","tsv",commons,outdir=outdir)
        different.to_csv(diff_name, sep='\t', index=False, encoding='utf-8')

        identical_name = get_output_name("identical","tsv",commons,outdir=outdir)
        identical.to_csv(identical_name, sep='\t', index=False, encoding='utf-8')

        unique_name = get_output_name("unique","tsv",commons,outdir=outdir)
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

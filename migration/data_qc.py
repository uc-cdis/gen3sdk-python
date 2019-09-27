# BRAIN QC Checks:
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

#import gen3
#from gen3.auth import Gen3Auth
#from gen3.submission import Gen3Submission
sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/gen3')
from submission import Gen3Submission
from auth import Gen3Auth
sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion')
from expansion import Gen3Expansion

#BRAIN Data Staging
api = 'https://data-staging.braincommons.org/'
profile = 'brain-staging'
creds = '/Users/christopher/Downloads/brain-staging-credentials.json'
auth = Gen3Auth(api,refresh_file=creds)
sub = Gen3Submission(api,auth)
exp = Gen3Expansion(api, auth)

prod_api = 'https://data.braincommons.org/'
prod_profile = 'bc'
prod_creds = '/Users/christopher/Downloads/bc-credentials.json'
prod_auth = Gen3Auth(prod_api,refresh_file=prod_creds)
prod_sub = Gen3Submission(prod_api,prod_auth)
prod_exp = Gen3Expansion(prod_api, prod_auth)

# %run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/migration/migration.py
# mig = Gen3Migration(api,auth)

# get the data dictionary

###############################################################
# 1) Download all the data in prod/staging
os.chdir("/Users/christopher/Documents/Notes/BHC/data_qc/")

# Get the TSV data
# project_ids = exp.get_project_ids()
# exp.get_project_tsvs(projects=project_ids,outdir='staging_tsvs_2',overwrite=False)
# prod_exp.get_project_tsvs(projects=project_ids,outdir='prod_tsvs_2',overwrite=False)

# 2) Create dictionary of stats for each property in each project TSV in both prod/staging.
staging_dir = "/Users/christopher/Documents/Notes/BHC/data_qc/staging_tsvs/"
sdd = sub.get_dictionary_all()

prod_dir = "/Users/christopher/Documents/Notes/BHC/data_qc/prod_tsvs/"
pdd = prod_sub.get_dictionary_all()

commons = {"staging":{"dir":staging_dir,"dd":sdd},"prod":{"dir":prod_dir,"dd":pdd}} # for each commons, give directory containing the project_tsvs and the data dictionary.

def summarize_tsv_data(
    commons,#a dictionary with nickname of each commons as keys, with each value a dictionary of project_tsvs directory ('dir') and the data dictionary ('dd')
    prefix='',#prefix of the project_tsvs directories, which should be the program name of the projects in the commons. Use `prefix=''` to get all the TSVs regardless of program.
    omit_props=['project_id','type','id','submitter_id','case_submitter_id','md5sum','file_name','object_id']
    ):#Properties to omit from being summarized. The results of props like object_id, md5sum, submitter_id are ~meaningless since all values are unique and would result in # bins=1 as N
    """ Adds a summary of TSV data to a dictionary consisting of the following data commons info:
        commons = {"name_of_commons":{"dir":dir_of_project_tsvs, "dd": data_dictionary_of_commons}}
        Can have as many commons.keys() as you like for different directories of TSVs to summarize.
    """

    dcs = list(commons.keys()) # get the names of the data commons, e.g., 'staging' and 'prod'

    for dc in dcs: #dc=dcs[0]

        project_count = 0 # count the projects in each "commons_tsvs" directory, e.g., output of Gen3Expansion.get_project_tsvs()
        node_count = 0 # count the nodes in each data set
        prop_count = 0 # count the properties in each data set

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
            project_count+=1

            os.chdir("{}{}".format(commons[dc]['dir'],project_dir))
            print("Changed working directory to:\n\t{}".format(os.getcwd()))

            fpattern = "{}*{}".format(prefix,'.tsv')
            fnames = glob.glob(fpattern)
            print("Found the following {} TSV templates in project {}:\n\n\t{}\n\n".format(len(fnames),project_id,fnames))

            for fname in fnames: # fname=fnames[0]

                node_regex = r"^" + re.escape(project_id) + r"_([a-zA-Z0-9_]+)\.tsv$" #node = re.search(r'^([a-zA-Z0-9_]+)-([a-zA-Z0-9]+)_([a-zA-Z0-9_]+)\.tsv$',fname).group(3)
                try:
                    node = re.search(node_regex, fname, re.IGNORECASE).group(1)
                    print("Extracting data from the '{}' node TSV.".format(node))
                    df = pd.read_csv(fname,sep='\t',header=0,dtype=str)
                except Exception as e:
                    print("\nCouldn't get node data for file:\n\t'{}'\n".format(fname))

                data[project_id][node] = {}
                node_count+=1

                total_records = len(df)

                regex = re.compile(r'^[A-Za-z0-9_]*[^.]$') #drop the links
                props = list(filter(regex.match, list(df)))

                for prop in omit_props: # filter properties (headers) in TSV to remove props we don't want to summarize
                    try:
                        props.remove(prop)
                    except Exception as e:
                        pass #not all nodes will have some props, like md5sum or file_name

                print("Total of {} records in TSV with {} properties.".format(total_records,len(props)))

                data[project_id][node]["properties"] = {} # all the property data stats in a node

                for prop in props: #prop=props[0]

                    data[project_id][node]["properties"][prop] = {}
                    prop_count+=1
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
                        #print("\t\tCalculating stats for property: {}".format(prop))
                        data[project_id][node]["properties"][prop]["stats"] = {"N":len(df[df[prop].notnull()])}
                        if ptype in ['string','enum','boolean']:
                            counts = Counter(df[df[prop].notnull()][prop])
                            df1 = pd.DataFrame.from_dict(counts, orient='index').reset_index()
                            bins = [tuple(x) for x in df1.values]
                            bins.sort(key=itemgetter(1),reverse=True)
                            data[project_id][node]["properties"][prop]["stats"]["bins"] = bins
                            print("\t\tTop 3 bins of {}: {}".format(bins[:3]))
                        elif ptype in ['number','integer']: #prop='concentration'
                            d = list(df[df[prop].notnull()][prop].astype(float))
                            data[project_id][node]["properties"][prop]["stats"]["min"] = min(d)
                            data[project_id][node]["properties"][prop]["stats"]["max"] = max(d)
                            data[project_id][node]["properties"][prop]["stats"]["mean"] = statistics.mean(d)
                            data[project_id][node]["properties"][prop]["stats"]["median"] = statistics.median(d)
                            if len(d) == 1:
                                data[project_id][node]["properties"][prop]["stats"]["stdev"] = "NA"
                            else:
                                data[project_id][node]["properties"][prop]["stats"]["stdev"] = statistics.stdev(d)
                            print("\t\t{}".format(data[project_id][node]["properties"][prop]["stats"]))
                        else:
                            print("\t\???? property type: {}".format(prop))
                            exit(1)

        commons[dc]['data'] = data #save data to commons 'data' after looping through all the TSVs from a commons
        commons[dc]['prop_count'] = prop_count
        commons[dc]['node_count'] = node_count
        commons[dc]['project_count'] = project_count
    return commons

commons = summarize_tsv_data(commons)
################################################################################
################################################################################
################################################################################
In [982]: commons['staging']['prop_count']
Out[982]: 14744
In [981]: commons['prod']['prop_count']
Out[981]: 12647
In [31]: commons['prod']['prop_count'] + commons['staging']['prop_count']
Out[31]: 27391

In [983]: commons['staging']['node_count']
Out[983]: 245
In [984]: commons['prod']['node_count']
Out[984]: 252
In [32]: commons['prod']['node_count'] + commons['staging']['node_count']
Out[32]: 497

In [985]: commons['prod']['project_count']
Out[985]: 12
In [986]: commons['staging']['project_count']
Out[986]: 12

In [770]: list(commons['staging']['data']['mjff-S4']['aliquot']['properties']['aliquot_volume']['stats'])
Out[770]: ['N', 'min', 'max', 'mean', 'median', 'stdev']

# lots of bins:
#commons: 'staging', project: 'mjff-BioFIND', node: 'medication', prop: 'who_drug_name'
In [778]: list(commons[dcs[1]]['data']['mjff-BioFIND']['medication']['properties']['who_drug_name']['stats'])
Out[778]: ['bins']

os.chdir("/Users/christopher/Documents/Notes/BHC/data_qc/")

def write_commons_report(commons, outdir='reports',bin_limit=False):
    """ Write a Data QC Report TSV
        where 'commons' is the dictionary output of 'summarize_data_commons'.
    """
    #outdir='reports'
    #bin_limit=25
    cmd = ['mkdir','-p',outdir]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
    except Exception as e:
        output = e.output.decode('UTF-8')
        print("ERROR:" + output)

    dcs = list(commons.keys())

    total_props = 0
    for dc in dcs:
        total_props += commons[dc]['prop_count']

    r = pd.DataFrame(index=range(0,total_props),
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
                    r['commons'][i] = dc
                    r['project'][i] = project
                    r['node'][i] = node
                    r['property'][i] = prop
                    r['total_records'][i] = data['total_records']
                    r['null_count'][i] = data['null_count']
                    r['property_type'][i] = data['property_type']
                    if 'stats' in data:
                        r['N'][i] = data['stats']['N']
                        if data['property_type'] in ['string','enum','boolean']:
                            prop_bins = data['stats']['bins']
                            bin_number = len(prop_bins.keys())
                            r['bin_number'][i] = bin_number
                            if not bin_limit is False and bin_number > bin_limit:
                                #r['bins'][i] = "Over bin limit ({}).".format(bin_limit)
                                #first_bins = {i: data['stats']['bins'][i] for i in list(data['stats']['bins'])[:2]}
                                # prop_bins = commons[dcs[1]]['data']['mjff-BioFIND']['medication']['properties']['who_drug_name']['stats']
                                # collections.Counter(prop_bins).most_common(10)
                                prop_bins = dict(collections.Counter(prop_bins).most_common(bin_limit))
                                #r['bins'][i] = "First {} bins: {}".format(bin_limit,str(prop_bins))
                                r['bins'][i] = prop_bins
                            else:
                                r['bins'][i] = prop_bins
                        elif data['property_type'] in ['integer','number']:
                            r['min'][i] = data['stats']['min']
                            r['max'][i] = data['stats']['max']
                            r['mean'][i] = data['stats']['mean']
                            r['median'][i] = data['stats']['median']
                            r['stdev'][i] = data['stats']['stdev']

                    i += 1

    report_name = 'report_'
    for i in range(len(dcs)):
        report_name += dcs[i]
        if i != len(dcs)-1:
            report_name += '_'
    report_name += '.tsv'

    outname = "{}/{}".format(outdir,report_name)
    r.to_csv(outname, sep='\t', index=False, encoding='utf-8')

    return r

report = write_commons_report(commons)

# 5) Compare prod/staging stats for each property in each project, considering breaking changes to data model. (e.g., properties that changed nodes/names, "age_at_enrollment" that changed distribution (took lowest value), etc.)



def compare_two_commons(report, outdir='reports',stats = ['total_records','null_count','N','bins','min','max','mean','median','stdev']):
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

    comp = {"identical":identical,"different":different}
    return comp


comparison = compare_two_commons(report)

###############################################################################
###############################################################################
# 6) Outlier analysis

def find_outliers(report):
    """ Finds outliers in the data.
    """

    return outliers

def get_nodes_props_from_dd(commons,time=False):
    dd_nodes = []
    uprops = []
    dprops = []
    node_regex = re.compile(r'^[a-zA-Z][A-Za-z0-9_]+$')
    for dd in [commons[dcs[0]]['dd'],commons[dcs[1]]['dd']]:
        dd_nodes = dd_nodes + list(dd)
        uprops = uprops + list(dd['_definitions']['ubiquitous_properties'].keys())
        dprops = dprops + list(dd['_definitions']['data_file_properties'].keys())
    dd_nodes = sorted(list(set(dd_nodes)))
    dd_nodes = list(filter(node_regex.search, dd_nodes))
    if not time: #remove time_of_ type nodes because usually have too many bins (unique strings)
        time_regex = re.compile(r'^time_[A-Za-z0-9_]+$')
        dd_nodes = [i for i in dd_nodes if not regex.match(i)]
    uprops = sorted(list(set(uprops)))
    dprops = sorted(list(set(dprops)))
    data = {"nodes":dd_nodes,"ubiquitous_properties":uprops,"data_file_properties":dprops}
    return data

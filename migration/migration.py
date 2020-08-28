import os, os.path, sys, subprocess, glob, json, re, operator, requests, datetime
import os.path
import fnmatch, sys, ntpath, copy
from shutil import copyfile
import numpy as np
from collections import Counter
from collections import OrderedDict
from statistics import mean
from pathlib import Path
import pandas as pd
from pandas.io.json import json_normalize
pd.options.mode.chained_assignment = None # turn off pandas chained assignment warning

import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

# import gen3
# from gen3.auth import Gen3Auth
# from gen3.submission import Gen3Submission

git_dir='/Users/christopher/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
from expansion.expansion import Gen3Expansion

class Gen3Error(Exception):
    pass

class Gen3Migration:
    """Scripts for migrating data in TSVs.
    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.
    Examples:
        This creates an instance of the Gen3Migration class pointed at the sandbox commons
        using the credentials.json downloaded from the commons profile page.
        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... mig = Gen3Migration(endpoint, auth)
    """

    def __init__(self, endpoint, auth_provider):
        self._auth_provider = auth_provider
        self._endpoint = endpoint
        self.sub = Gen3Submission(endpoint, auth_provider)
        self.exp = Gen3Expansion(endpoint, auth_provider, self.sub)

    def read_tsv(self,project_id,node,name='temp',strip=True):

        if name is not None:
            filename = "{}_{}_{}.tsv".format(name,project_id,node)
        else:
            filename = "{}_{}.tsv".format(project_id,node)

        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
            if strip is True:
                df.columns = df.columns.str.replace("#[0-9]+", "")

        except Exception as e:
            print("\tError reading '{}' TSV: {}".format(filename,e))
            return

        #warn if there are duplicate submitter_ids:
        if 'submitter_id' in df:
            sids = list(df['submitter_id'])
            if len(sids) != len(set(sids)):
                print("\n\n\n\t\tWARNING!! This TSV contains duplicate submitter_ids: {}".format(filename))

        return df


    def write_tsv(self,df,project_id,node,name='temp'):
        if name is not None:
            outname = "{0}_{1}_{2}.tsv".format(name,project_id,node)
        else:
            outname = "{0}_{1}.tsv".format(project_id,node)
        try:
            df.to_csv(outname, sep='\t', index=False, encoding='utf-8')
            print("\t\t\tTotal of {} records written to node '{}' in file: {}.".format(len(df),node,outname))
        except Exception as e:
            print("\t\t\tError writing TSV file: {}".format(e))
        return df

    def make_temp_files(self,prefix,suffix,name='temp',overwrite=True,nodes=['all']):
        """
        Make copies of all files matching a pattern with "temp_" prefix added.
        Args:
            prefix(str): A substring at the beginning of file names.
            suffix(str): A substring at the end of file names.
            name(str): The substring to add at the beginning of copies.
        Example:
            This makes a copy of every TSV file beginning with "DEV" (that is, files matching DEV*tsv) and names the copies temp_DEV*tsv.
            make_temp_files(prefix='DEV',suffix='tsv')
        """
        if isinstance(nodes, str):
            nodes = [nodes]

        if nodes == ['all']:
            pattern = "{0}*{1}".format(prefix,suffix)
            filenames = glob.glob(pattern)

        elif isinstance(nodes, list):
            filenames = []
            for node in nodes:
                pattern = "{0}*{1}.{2}".format(prefix,node,suffix)
                node_files = glob.glob(pattern)
                if len(node_files) > 0:
                    filenames.append(glob.glob(pattern)[0])
                else:
                    print("\tNo '{}' node TSV found with prefix '{}'.".format(node,prefix))

        else:
            raise Gen3Error("Please provide 'nodes' argument as a string or list of node_ids:\n\t{}\n\tis not properly formatted.".format(nodes))

        if overwrite is True:
            for filename in filenames:
                temp_name = "{}_{}".format(name,filename)
                print("\tCopying file {0} to:\n\t\t{1}".format(filename,temp_name))
                copyfile(filename,temp_name)
            print("\tTotal of {} '{}' files created.".format(len(filenames),name))

        pattern = "{}*{}".format(name,suffix)
        tempfiles = glob.glob(pattern)
        print("\tReturning list of {} '{}' files found in this directory.".format(len(tempfiles),name))
        return tempfiles

    def check_null(self,project_id,node,prop,name='temp'):
        """
        Checks if all data for a prop in a TSV are null.
        Returns the number of non-null records in the TSV.
        If check_null(project,node,prop) == 0, all data are null.
        """
        print("\tChecking for non-null data in '{}' prop of '{}' node in project '{}'.".format(prop,node,project_id))
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        filename = "{}_{}_{}.tsv".format(name,project_id,node)
        if prop in list(df):
            nn = df.loc[df[prop].notnull()] #number of non-null records
            print("\t\tNon-null values count: {}.".format(len(nn)))
            return len(nn)
        else:
            print("No header '{}' in the TSV for node '{}' of project '{}'.".format(prop,node,project_id))
            print(list(df))
            return 0

    def get_non_null(self,project_id,node,prop,name='temp'):
        """
        Returns list of non-null data for a prop in a TSV.
        """
        print("\tChecking for non-null data in '{}' prop of '{}' node in project '{}'.".format(prop,node,project_id))
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        filename = "{}_{}_{}.tsv".format(name,project_id,node)
        if prop in list(df):
            nn = list(df.loc[df[prop].notnull()][prop]) #number of non-null records
            print("\t\tNon-null values count: \n\t{}.".format(nn))
            return nn
        else:
            print("No header '{}' in the TSV for node '{}' of project '{}'.".format(prop,node,project_id))
            print(list(df))
            return 0

    def merge_nodes(self,project_id,in_nodes,out_node,name='temp'):
        """
        Merges a list of node TSVs into a single TSV.
        Args:
            in_nodes(list): List of node TSVs to merge into a single TSV.
            out_node(str): The name of the new merged TSV.
        """
        print("\tMerging nodes {} to '{}'.".format(in_nodes,out_node))
        dfs = []
        for node in in_nodes:
            filename = "{}_{}_{}.tsv".format(name,project_id, node)
            try:
                df1 = pd.read_csv(filename,sep='\t', header=0, dtype=str)
                dfs.append(df1)
                print("\t{} node found with {} records.".format(node,len(df1)))
            except Exception as e:
                print("\tCan't read file {}: {}".format(filename,e))
                pass
        if len(dfs) == 0:
            print("\tNo nodes were found to merge.")
        else:
            df = pd.concat(dfs,ignore_index=True,sort=False)
            df['type'] = out_node
            outname = "{}_{}_{}.tsv".format(name, project_id, out_node)
            df.to_csv(outname, sep='\t', index=False, encoding='utf-8')
            print("\tTotal of {} records written to node {} in file {}.".format(len(df),out_node,outname))
            return df

    def merge_props_old(self,project_id,node,props,name='temp'):
        """
        This function merges a list of properties (column headers in a single node TSV) into one new or existing destination property.
        The properties merged are then dropped from the TSV's column headers.
        The fxn will check if the destination property already exists in the TSV column headers and whether any non-null data will be overwritten by merging the data.

        Args:
            project_id(str): The project_id of the TSV.
            node(str): The node TSV containing properties to merge.
            props(dict): A dictionary of "destination_prop":["list","of","props","to","merge","and","drop"]
        """
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        dropped = []
        for prop in list(props.keys()): # keys are destination props
            if prop not in list(df): # if destination prop doesn't exist in TSV, add it with all null data
                df[prop] = np.nan


            old_props = props[prop] # list of properties to merge
            for old_prop in old_props:
                if old_prop in list(df):
                    df_old = df.loc[df[old_prop].notnull()]
                    df_old[prop] = df_old[old_prop]
                    df_rest = df.loc[df[old_prop].isnull()]
                    df_merged = pd.concat([df_rest,df_old],ignore_index=True,sort=False)
                    df = df_merged.drop(columns=[old_prop])
                    dropped.append(old_prop)
                    print("\tprop '{}' merged into '{}' and dropped from '{}' TSV.".format(old_prop,prop,node))
                else:
                    print("\tprop '{}' not found in '{}' TSV. Skipping...".format(old_prop,node))
        if len(dropped) > 0:
            print("\tprops {} merged into {}.".format(dropped,list(props.keys())))
            df = self.write_tsv(df,project_id,node)
            return df
        else:
            print("\tNo props dropped from '{}'. No TSV written.".format(node))
            return

    def merge_props(self,project_id,node,dest_prop,merge_props,name='temp'):
        """
        This function merges a list of properties (column headers in a single node TSV) into one new or existing destination property.
        The properties merged are then dropped from the TSV's column headers.
        The fxn will check if the destination property already exists in the TSV column headers and whether any non-null data will be overwritten by merging the data.

        Args:
            project_id(str): The project_id of the TSV.
            node(str): The node TSV containing properties to merge.
            dest_prop(str): The new or existing property (or TSV column header) to merge data from "merge_props" into.
            merge_props(list): A list properties to merge into the destination property.

        Example:
            node,dest_prop,merge_props = 'demographic','marital_or_partner_status','marital_status'
            df = prep_mig.merge_props(project_id=project_id,node=node,dest_prop=dest_prop,merge_props=merge_props,name=name)
        """

        df = self.read_tsv(project_id=project_id,node=node,name=name)

        #check that each of the "merge_props" exists in the TSV; if not, return original dataframe
        if isinstance(merge_props, str):
            merge_props = [merge_props]
        if not isinstance(merge_props,list):
            print("\tPlease provide a list of properties (TSV column headers) to merge into the destination property!!")
        else:
            for merge_prop in merge_props:
                if not merge_prop in list(df):
                    print("\t\tProperty to merge '{}' is not found in the '{}' TSV of project '{}'!".format(merge_prop,node,project_id))
                    return df

        # Next check for conflicts to avoid overwriting data:
        print("\tAttempting to merge '{}' data into '{}' in '{}' TSV of project '{}'.".format(merge_props,dest_prop,node,project_id))

        nn_dfs = []

        # check if destination property already exists in TSV and if so, whether prop has any non-null data
        if dest_prop in list(df):
            dest_nn = df.loc[df[dest_prop].notnull()][['submitter_id',dest_prop]+merge_props] # make a series of non-null destination data, if any
            if not dest_nn.empty: #if destination property has non-null data
                nn_dfs.append(dest_nn)
                print("\t\t{} non-null values for destination prop '{}' in '{}' TSV.".format(len(dest_nn),dest_prop,node))
        else:
            df[dest_prop] = np.nan # if dest_prop is not in the TSV, add it with all null values

        # make series of non-null data to merge
        for merge_prop in merge_props:
            merge_nn = df.loc[df[merge_prop].notnull()][['submitter_id',dest_prop]+merge_props]
            print("\t\t{} non-null values out of {} total records for prop to merge '{}' in '{}' TSV of project '{}'.".format(len(merge_nn),len(df),merge_prop,node,project_id))
            if not merge_nn.empty:
                nn_dfs.append(merge_nn)

        nn = pd.concat(nn_dfs,ignore_index=True,sort=False)
        conflicts = nn.loc[nn[['submitter_id']].duplicated()]

        if not conflicts.empty:
            print("\n\t\tConflicts in data! If you proceed, existing data will be overwritten!\n\n{}".format(conflicts))
            return conflicts

        # if no conflicts, then merge the data into the new prop and drop the old props
        dropped = []
        mdf = copy.deepcopy(df)
        for nn_df in nn_dfs:
            try:
                mdf[dest_prop] = nn_df[merge_prop]
                mdf.drop(columns=[merge_prop],inplace=True)
                dropped.append(merge_prop)
                print("\t\tprop '{}' merged into '{}' and dropped from '{}' TSV.".format(merge_prop,dest_prop,node))
            except Exception as e:
                print("\tUnable to merge '{}' prop data into '{}' prop in '{}' TSV:\n\t\t{}".format(merge_prop,dest_prop,node,e))

        if len(dropped) > 0:
            print("\t\tprops {} merged into destination prop '{}' of '{}' TSV in project '{}'.".format(dropped,dest_prop,node,project_id))
            self.write_tsv(mdf,project_id,node)
        else:
            print("\t\tNo props dropped from '{}'. No TSV written.".format(node))

        return mdf


    def add_missing_links(self,project_id,node,link,old_parent=None,links=None, name='temp'):
        """
        This function adds missing links to a node's TSV when the parent node changes.
        Args:
            node (str): This is the node TSV to add links to.
            link (str): This is the name of the node to add links to.
        Example:
            This adds missing links to the visit node to the imaging_exam TSV.
            add_missing_links(node='imaging_exam',link='visit')
        """
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(project_id, node)

        link_name = "{}s.submitter_id".format(link)
        if link_name not in list(df):
            df[link_name] = np.nan
        df_no_link = df.loc[df[link_name].isnull()] # records with no visits.submitter_id
        if len(df_no_link) > 0:
            df_no_link[link_name] = df_no_link['submitter_id'] + "_{}".format(link) # visit submitter_id is "<ESID>_visit"
            df_link = df.loc[df[link_name].notnull()]
            df_final = pd.concat([df_link,df_no_link],ignore_index=True,sort=False) # Merge dummy visits back into original df
            df_final.to_csv(filename, sep='\t', index=False, encoding='utf-8')
            print("\t{} links to '{}' added for '{}' in TSV file: {}".format(str(len(df_no_link)),link,node,filename))
            return df_final
        else:
            print("\tNo records are missing links to '{}' in the '{}' TSV.".format(link,node))
            return

    def create_missing_links(self,project_id,node,link,old_parent,props,new_dd,old_dd,links=None,name='temp'):
        """
        This fxn creates links TSV for links in a node that don't exist.
        Args:
            node(str): This is the node TSV in which to look for links that don't exist.
            link(str): This is the node to create the link records in.
            old_parent(str): This is the backref of the parent node of 'node' prior to the dictionary change.
            props(dict): Dict of required props/values to add to new link records.
        Example:
            This will create visit records that don't exist in the visit TSV but are in the imaging_exam TSV.
            create_missing_links(node='imaging_exam',link='visit',old_parent='cases',props={'visit_label':'Imaging','visit_method':'In-person Visit'},new_dd=dd,old_dd=prod_dd,links=None)
            create_missing_links(node='diagnosis',link='visit',old_parent='cases',props={'visit_label':'Unknown','visit_method':'Unknown'},new_dd=dd,old_dd=prod_dd)
        """
        print("\tCreating missing '{}' records with links to '{}' for '{}'.".format(link,old_parent,node))

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        link_name = "{}s.submitter_id".format(link) # visits.submitter_id
        if link_name in list(df):
            link_names = list(df[link_name])
        else:
            link_names = []
        link_file = "{}_{}_{}.tsv".format(name,project_id,link)
        try:
            link_df = pd.read_csv(link_file,sep='\t',header=0,dtype=str) #open visit TSV
            existing = list(link_df['submitter_id']) # existing visits
            missing = set(link_names).difference(existing) # visit links in df missing in visit TSV: lists items in link_names missing from existing
            if len(missing) > 0:
                print("\t\tCreating {} records in '{}' with links to same cases as '{}' for missing '{}' links.".format(len(missing),link,old_parent,node))
            else:
                print("\t\tAll {} records in '{}' node have existing links to '{}'. No new records added.".format(len(df),node,link))
                return link_df.loc[link_df['submitter_id'].isin(link_names)]
        except Exception as e:
            link_df = pd.DataFrame()
            print("\t\tCouldn't read '{}' TSV: {}\nCreating new TSV for links.".format(link,e))
            missing = link_names
        parent_link = "{}.submitter_id".format(old_parent)
        if parent_link in list(df):
            new_links = df.loc[df[link_name].isin(missing)][[link_name,parent_link]]
        else:
            parent_link = "{}.submitter_id#1".format(old_parent)
            new_links = df.loc[df[link_name].isin(missing)][[link_name,parent_link]]
        new_links = new_links.rename(columns={link_name:'submitter_id'})
        new_links['type'] = link
        for prop in props:
            new_links[prop] = props[prop]
        if old_parent is not 'cases':
            old_links = old_dd[node]['links']
            for old_link in old_links:
                if old_link['name'] == old_parent:
                    old_node = old_link['target_type']
            old_name = "{}_{}_{}.tsv".format(name,project_id,old_node)
            try:
                odf = pd.read_csv(old_name,sep='\t',header=0,dtype=str)
            except Exception as e:
                print("\t\tCouldn't read '{}' TSV: '{}'. Skipping...".format(node,e))
                return
            # df1 = df_no_link.loc[df_no_link[old_link].notnull()]
            # if len(df1) > 0:
            if 'cases.submitter_id' in list(odf):
                case_links = odf[['cases.submitter_id','submitter_id']]
                case_links.rename(columns={'submitter_id':parent_link}, inplace=True)
                new_links = pd.merge(new_links,case_links,on=parent_link, how='left')
                new_links.drop(columns=[parent_link],inplace=True)
            else:
                old_backref = links[old_node][0]
                old_links2 = old_dd[old_node]['links']
                for old_link2 in old_links2:
                    if old_link2['name'] == old_backref:
                        old_node2 = old_link2['target_type']
                old_name2 = "{}_{}_{}.tsv".format(name,project_id,old_node2)
                try:
                    odf1 = pd.read_csv(old_name2,sep='\t',header=0,dtype=str)
                except Exception as e:
                    print("\tCouldn't read '{}' TSV: '{}'. Skipping...".format(node,e))
                    return
                odf[parent_link] = odf.submitter_id
                old_parent_link = "{}.submitter_id".format(old_backref)
                if old_parent_link in list(odf):
                    odf.submitter_id = odf[old_parent_link]
                else:
                    old_parent_link = "{}.submitter_id#1".format(old_backref)
                    odf.submitter_id = odf[old_parent_link]
                odf2 = pd.merge(odf,odf1,on='submitter_id', how='left')
                case_links = odf2[['cases.submitter_id',parent_link]]
                new_links = pd.merge(new_links,case_links,on=parent_link, how='left')
                new_links.drop(columns=[parent_link],inplace=True)
        all_links = pd.concat([link_df,new_links],ignore_index=True,sort=False)
        all_links.to_csv(link_file,sep='\t',index=False,encoding='utf-8')
        print("\t{} new missing '{}' records saved into TSV file:\n\t\t{}".format(str(len(new_links)),link,link_file))
        return new_links

    def batch_add_visits(self,project_id,new_dd,old_dd,links):
        """
        Adds 'Unknown' dummy visits to records in nodes that link to the 'case' node and have no link to the 'visit' node.
        Args:
            project_id(str): The project_id of the TSVs.
            new_dd(dict): The new data dictionary. Get it with `dd=sub.get_dictionary_all()`.
            old_dd(dict): The old data dictionary (e.g., in production). Get it with `dd=prod_sub.get_dictionary_all()`.
            links(dict): A dict of nodes with links to remove, e.g., {'node':['link1','link2']}.
        Example:
            This adds 'visits.submitter_id' links to the 'allergy' node, and it then adds those new visits to the 'visit' TSV, lining the new visit records to the same 'case' records the 'allergy' records are linked to.
            batch_add_visits(project_id=project_id,links={'allergy': ['cases', 'treatments', 'medications']}
        """
        required_props={'visit_label':'Unknown','visit_method':'Unknown'}
        total = 0
        dfs = []
        for node in list(links.keys()):
            # if the node has (only) a link to visit in new dd:
            targets = []
            node_links = new_dd[node]['links']
            for link in node_links:
                if 'subgroup' in list(link):
                    for subgroup in link['subgroup']:
                        targets.append(subgroup['target_type'])
                elif 'target_type' in list(link):
                    targets.append(link['target_type'])
            links_to_drop = links[node]
            print("\t{}: links {}, dropping {}".format(node,targets,links_to_drop))
            if 'cases' not in links_to_drop and len(links_to_drop) == 1 and 'visit' in targets and len(targets) == 1:
                df = self.add_missing_links(project_id=project_id,node=node,link='visit')
                if df is not None:
                    df = self.create_missing_links(project_id=project_id,node=node,link='visit',old_parent=links_to_drop[0],props=required_props,new_dd=new_dd,old_dd=old_dd,links=links)
                    dfs.append(df)
                    total += len(df)
            elif 'cases' in links_to_drop and 'visit' in targets and len(targets) == 1:
                df = self.add_missing_links(project_id=project_id,node=node,link='visit')
                if df is not None:
                    df = self.create_missing_links(project_id=project_id,node=node,link='visit',old_parent='cases',props=required_props,new_dd=new_dd,old_dd=old_dd,links=links)
                    dfs.append(df)
                    total += len(df)
            else:
                print("\tNo links to 'case' found in the '{}' TSV.".format(node))
        if len(dfs) > 0:
                df = pd.concat(dfs,ignore_index=True,sort=False)
        print("\tTotal of {} missing visit links created for this batch.".format(total))
        return df


    def move_prop(self,project_id,old_node,new_node,prop,dd,parent_node=None,required_props=None,name='temp',warn=True):
        """
        This function moves a single property and its data from one node (old_node) to another node (new_node).
        Fxn also checks whether the data to be moved actually has non-null data. If all data are null, no new records are created.
        Fxn also checks whether the property already exists in the new_node, and if it does, fxn exits with nothing changed.

        Args:
            old_node(str): Node TSV to copy data from.
            new_node(str): Node TSV to add copied data to.
            prop: A property, or column header in the old_node TSV, which contains data to move to the new_node.
            dd(dict): The data dictionary being used to determine node relationships; get it with Gen3Submission.get_dictionary_all() fxn.
            parent_node(str): The parent node that links the old_node to the new_node, e.g., 'visit' or 'case'.
            required_props(dict): If the new_node has additional required props, enter the value all records should get for each key.

        Example:
            This moves the prop 'military_status' from 'demographic' node to 'military_history' node, which should link to the same parent node 'case'.
            move_prop(from_node='demographic',to_node='military_history',prop='military_status',parent_node='case')
        """

        print("\tMoving prop '{}' from node '{}' to '{}'.".format(prop,old_node,new_node))

        odf = self.read_tsv(project_id,old_node)

        if odf is None:
            print("\t\tNo '{0}' TSV found in project '{1}'. Nothing changed.".format(old_node,project_id))
            return

        # Check that the data column to move exists and is not entirely null. # If there are no non-null data, then there is no reason to move the TSV header.
        if prop not in odf:
            print("\t\t'{}' not found in '{}' TSV. Nothing changed.".format(prop,old_node))
            return odf

        onn = odf.loc[odf[prop].notnull()]

        if len(onn) == 0:
            print("\t\tAll null data in '{}' node for '{}' prop. No data to migrate; '{}' TSV unchanged.".format(old_node,prop,new_node))
            print("\t\tDropping '{}' from old_node: '{}' TSV.".format(prop,old_node))
            self.write_tsv(odf.drop(columns=[prop]),project_id,old_node)
            return odf

        # if the new node TSV already exists, read it in, if not, create a new df
        ndf = self.read_tsv(project_id,new_node)
        if ndf is not None:
            print("\t\t'{}' TSV already exists with {} records.".format(new_node,len(ndf)))
            new_file = False
        else:
            ndf = pd.DataFrame(columns=['submitter_id'])
            print("\t\tNo '{}' TSV found. Creating new TSV for data to be moved.".format(new_node))
            new_file = True

        # Check that prop isn't already in the new_node with non-null data. If it is and 'warn' isn't set to False, then stop.
        if prop in ndf:
            nn = ndf.loc[ndf[prop].notnull()]
            if not nn.empty and warn is True:
                print("\n\n\t\t\tWARNING! prop '{}' already exists in '{}' with {} non-null records.".format(prop,new_node,len(nn)))
                return ndf
            else:
                ndf.drop(columns=[prop],inplace=True,errors='raise')
                print("\t\t\t'{}' already found in '{}' TSV with all null data; dropping column from TSV.".format(prop,new_node))
        else:
            print("\t\t\tProp '{}' not in original '{}' TSV. Adding now.".format(prop,new_node))

        # set the parent_link and check for records with no link
        if parent_node is not None:
            parent_link = "{}s.submitter_id".format(parent_node)
        else:
            parent_link = "{}s.submitter_id".format(new_node)

        no_link = odf.loc[odf[parent_link].isnull()] # from_node records with no link to parent_node
        if not no_link.empty: # if there are records with no links to parent node
            print("\tWarning: there are {} '{}' records with no links to parent '{}' node!".format(len(from_no_link),from_node,parent_node))
            return odf

        #p = dict(zip(odf[parent_link],odf[prop]))

        # Determine which header to merge data on (usually link to the parent node, but for some many-to-one relationships, may need to merge data using visit_id (dm 2.2.))
        pdf = onn[[parent_link,prop]] # prop dataframe
        pdf.drop_duplicates(subset=None, keep='first', inplace=True)
        if len(onn) != len(pdf): # if number of non-null records doesn't equal number of unique IDs for those records, then data are many-to-one with parent node. Try merging on visit_id
            print("\t\t\tCan't move data using '{}'!. More than one '{}' value found per '{}' in '{}' TSV. Total non-null records: {}. Total of unique '{}' for these records: {}. Attemping to merge on 'visit_id'.".format(parent_link,prop,parent_link,old_node,len(onn),parent_link,len(pdf)))
            if 'visit_id' in onn and 'visit_id' in ndf:
                old_visits = list(set(onn['visit_id']))
                new_visits = list(set(ndf['visit_id']))
                if len(old_visits) > 0 and len(new_visits) > 0:
                    matching_visits = list(set(old_visits).intersection(new_visits))
                    missing_visits = list(set(old_visits).difference(new_visits))
                    if len(matching_visits) == len(old_visits):
                        merge_on = 'visit_id'
                        print("\t\t\tFound {} matching visit_ids between records in '{}' and '{}' TSVs. Merging '{}' into '{}' on 'visit_id'.".format(len(matching_visits),old_node,new_node,prop,new_node))
                    elif len(matching_visits) > 0:
                        print("\t\t\tFound some BUT NOT ALL matching visit_ids between records in '{}' and '{}' TSVs. Merging {} matching records '{}' into '{}' on 'visit_id'.".format(old_node,new_node,len(matching_visits),prop,new_node))
                        merge_on = 'visit_id'
                    else: # if len(matching_visits) == 0:
                        print("\t\t\tFound no matching visit_ids between records in '{}' and '{}' TSVs!".format(old_node,new_node,prop,new_node,parent_link))
                        return odf
            else:
                print("\t\t\t\tCan't merge on 'visit_id'! Both old and new nodes must have 'visit_id' in TSV to move data on 'visit_id'.".format(parent_link))
        else:
            merge_on = parent_link

        pdf = onn[[merge_on,prop]] # prop dataframe
        pdf.drop_duplicates(subset=None, keep='first', inplace=True)

        old_links = list(set(onn[merge_on]))
        new_links = list(set(ndf[merge_on]))
        matching_links = list(set(old_links).intersection(new_links))
        missing_links = list(set(old_links).difference(new_links))
        if len(matching_links) != len(old_links):
            print("\n\n\n\n\t\t\tWARNING! Found only {} matches in {} '{}' links in '{}' and '{}' TSVs. \n\n\n\t\t\tMAY WANT TO LOOK INTO THIS!\n\n\n\n".format(len(matching_links),len(old_links),parent_link,old_node,new_node,prop,new_node,parent_link))

        #create entirely new records for old_node data without links already in new_node TSV
        if len(missing_links) > 0:
            new = pdf.loc[pdf[merge_on].isin(missing_links)]
            new['submitter_id'] = new[merge_on] + "_{}".format(prop)
            new['project_id'] = project_id
            new['type'] = new_node
            ndf = pd.concat([ndf,new],ignore_index=True,sort=False)

        if new_node is 'case':
            pdf = odf[[parent_link,prop]] # prop dataframe
            pdf.drop_duplicates(subset=None, keep='first', inplace=True)
            pdf.rename(columns={parent_link:'submitter_id'},inplace=True,errors='raise')
            headers = list(pdf)
            case_data = pd.DataFrame(columns=headers)
            case_ids = list(pdf['submitter_id'].unique())
            case_data['submitter_id'] = case_ids
            count = 1
            for case_id in case_ids:
                print("\tGathering unique data for case '{}' ({}/{})".format(case_id,count,len(case_ids)))
                df1 = pdf.loc[pdf['submitter_id']==case_id]
                for header in headers:
                    vals = list(set(df1.loc[df1[header].notnull()][header].unique()))
                    if len(vals) == 1:
                        case_data.loc[case_data['submitter_id']==case_id,header] = vals
                    elif len(vals) > 1:
                        print("\t{}: {}".format(header,vals))
                        if header == 'age_at_enrollment': # special case hard-coded for BRAIN Commons migration
                            lowest_val = min(vals, key=float)
                            print("\tSelecting lowest value '{}' from {}.".format(lowest_val,vals))
                            case_data.loc[case_data['submitter_id']==case_id,header] = lowest_val
                count += 1
            df = pd.merge(pdf,case_data,on='submitter_id', how='left')

        elif parent_node is 'case':
            df = pd.merge(left=ndf, right=pdf, how='left', left_on=merge_on, right_on=merge_on)
            print("\t\tMerging {} non-null '{}' records into '{}' TSV on '{}'.".format(len(pdf),prop,new_node,merge_on))

        else: # neither new_node nor parent_node are 'case'
            df['submitter_id'] = odf['submitter_id'] + "_{}".format(new_node)
            df['type'] = new_node
            df['project_id'] = project_id
            df = df.loc[~df['submitter_id'].isin(list(ndf.submitter_id))]
            df = pd.concat([ndf,df],ignore_index=True,sort=False)

        # add any missing required props to new DF
        if required_props is not None:
            new_required = list(set(list(dd[new_node]['required'])).difference(list(df)))
            link_target = None
            for link in list(dd[new_node]['links']):
                if link['name'] in new_required:
                    link_target = link['target_type']
                    new_required.remove(link['name'])
            for prop in new_required:
                if prop in list(required_props.keys()):
                    df[prop] = required_props[prop]
                    print("\tMissing required prop '{}' added to new '{}' TSV with all {} values.".format(prop,new_node,required_props[prop]))
                else:
                    df[prop] = np.nan
                    print("\tMissing required prop '{}' added to new '{}' TSV with all null values.".format(prop,new_node))

        # CHECK the migration!
        null = df.loc[df[prop].isnull()]
        null_parents = list(set(null[merge_on]))
        onn_parents = list(set(onn[merge_on]))
        missed = list(set(null_parents).intersection(onn_parents)) #list of ids (type of id is 'merge_on') for records in df where there is a value in old TSV but not in new TSV
        nn = df.loc[df[prop].notnull()][[merge_on,prop]]
        nn.drop_duplicates(subset=None, keep='first', inplace=True)

        if len(nn) != len(onn) or len(missed) > 0:
            print("\t\t\t\n\nWARNING! It appears some data in old node '{}' ({} non-null records) was not properly migrated to new node '{}' ({} non-null records)!\n\n".format(old_node,len(onn),new_node,len(nn)))

        try:
            print("\t\tDropped '{}' from old_node: '{}' TSV.".format(prop,old_node))
            self.write_tsv(odf.drop(columns=[prop]),project_id,old_node)
            print("\t\tMoved prop '{}' with {} non-null records from '{}' TSV to '{}'.".format(prop,len(onn),old_node,new_node))
            self.write_tsv(df,project_id,new_node)

        except Exception as e:
            print(type(e),e)

        return df





    def add_prop(self,project_id,node,props):

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        for prop in list(props.keys()):
            if prop not in list(df):
                df[prop] = props[prop]
            else:
                print("\tprop '{}' already in the TSV for node '{}'.".format(prop,node))

        df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
        return df


    def non_null_data(self,project_id,node,prop,name='temp'):
        """ Returns the non-null data for a property.
        """
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        nn = df.loc[df[prop].notnull()]
        return nn


    def check_non_null(self,project_id,node,props,name='temp'):

        non_null = {}
        df = self.read_tsv(project_id=project_id,node=node,name=name)

        if df is not None:
            print("\tChecking data in '{}' TSV of project '{}'".format(node,project_id))

        for prop in props:
            if df is not None:
                if prop in df:
                    nn = df.loc[df[prop].notnull()]
                    non_null[prop] = len(nn)
                    print("\t\tprop '{}' has '{}' non-null records".format(prop,len(nn)))
                else:
                    non_null[prop] = 'NO_PROP'
                    print("\t\tprop '{}' not found in TSV".format(prop))
            else:
                non_null[prop] = 'NO_TSV'
        return non_null


    def check_props_data(self,tsv_dir,props,compare=False,project_ids='all',name='temp',outname='prod'):
        """ Creates a report of null and non-null data counts for a given list of properties.
            If 'compare' is True, will print a report of comparisons
        """
        try:
            os.chdir(tsv_dir)
        except Exception as e:
            print("Couldn't locate TSV dir ({}): {}".format(tsv_dir,e))

        if project_ids == 'all':
            pattern = "*_tsvs"
            project_dirs = glob.glob(pattern)
        else:
            project_dirs = [pdir+'_tsvs' for pdir in project_ids]

        proj_regex = re.compile("(.+)_tsvs")
        projects = {proj_regex.match(project_dir).groups()[0]: project_dir for project_dir in project_dirs}

        data = {}
        nodes = list(props)

        for project_id in list(projects):

            pdir = "{}/{}".format(tsv_dir,projects[project_id])
            os.chdir(pdir)
            data[project_id] = {}

            for node in nodes:
                data[project_id][node] = {}
                df = self.read_tsv(project_id=project_id,node=node,name=name)
                if df is not None:
                    print("\tChecking data in '{}' TSV of project '{}'".format(node,project_id))
                node_props = props[node]
                for prop in node_props:
                    data[project_id][node][prop] = {}
                    if df is None:
                        missing = 'NO_TSV'
                        print("\t\tTSV '{}' not found in {}".format(node,pdir))
                    else:
                        if prop not in df:
                            missing = 'NO_PROP'
                            print("\t\tprop '{}' not found in TSV".format(prop))
                        else:
                            missing = False
                    data[project_id][node][prop]['missing'] = missing

                    # if prop is missing, all values get 0
                    if missing is 'NO_PROP' or missing is 'NO_TSV':
                        data[project_id][node][prop]['N'] = 0
                        data[project_id][node][prop]['nn'] = 0
                        data[project_id][node][prop]['null'] = 0
                        data[project_id][node][prop]['value_count'] = 0

                    else:
                        data[project_id][node][prop]['N'] = len(df)
                        nn = df.loc[df[prop].notnull()]
                        data[project_id][node][prop]['nn'] = len(nn)
                        null = df.loc[df[prop].isnull()]
                        data[project_id][node][prop]['null'] = len(null)
                        if nn.empty:
                            value_count = 0
                        else:
                            values = list(set(nn[prop]))
                            value_count = len(values)
                        data[project_id][node][prop]['value_count'] = value_count

                        print("\t\tprop '{}' has {} total, {} non-null, {} null records, and {} unique values.".format(prop,len(df),len(nn),len(null),value_count))

        os.chdir(tsv_dir)

        if compare is False:

            c = {'project_id':[],'node':[],'prop':[],
                'missing':[],'N':[],
                'null':[],'nn':[],'value_count':[]}

            for node in nodes:
                node_props = props[node]
                for prop in node_props:
                    for project_id in list(projects):
                        missing = data[project_id][node][prop]['missing']
                        N = data[project_id][node][prop]['N']
                        null = data[project_id][node][prop]['null']
                        nn = data[project_id][node][prop]['nn']
                        value_count = data[project_id][node][prop]['value_count']
                        c['project_id'].append(project_id)
                        c['node'].append(node)
                        c['prop'].append(prop)
                        c['missing'].append(missing)
                        c['N'].append(N)
                        c['null'].append(null)
                        c['nn'].append(nn)
                        c['value_count'].append(value_count)
            outfile = 'check_props_data_{}.tsv'.format(outname)

        else: #if compare is True:
            c = {'project_id':[],'old_node':[],'old_prop':[],'new_node':[],'new_prop':[],'old_count':[],'new_count':[],'conflict':[]}
            for pair in compare:
                for project_id in list(projects):
                    ((old_node,old_prop),(new_node,new_prop)) = pair
                    # old_count = data[project_id][old_node][old_prop]
                    new_count = data[project_id][new_node][new_prop]
                    c['project_id'].append(project_id)
                    c['old_node'].append(old_node)
                    c['old_prop'].append(old_prop)
                    c['old_count'].append(old_count)
                    c['new_node'].append(new_node)
                    c['new_prop'].append(new_prop)
                    c['new_count'].append(new_count)
                    if old_count != 'NO_TSV' and old_count != 0 and old_count != 'NO_PROP': # data exists in old_prop
                        if new_count != 'NO_TSV' and new_count != 0 and new_count != 'NO_PROP': # data exists in new_prop
                            conflict = True
                    else:
                        conflict = False
                    c['conflict'].append(conflict)

        try:
            outfile = '{}_compare_props_data.tsv'.format(outname)
            cdf = pd.DataFrame(c)
            cdf.to_csv(outfile,sep='\t',index=False)
            conflicts = cdf.loc[cdf['conflict']==True]
            if not conflicts.empty:
                print("\n\n\n{} conflicts found in data!\n".format(len(conflicts)))
                display(conflicts)
            return cdf

        except Exception as e:
            print("\t\t{}: {}".format(type(e),e))
            print("\n\t\tc = '{}'".format(c))
            return c


    def change_prop_name(self,project_id,node,props,name='temp',force=False):
        """
        Changes the name of a column header in a single TSV.
        Checks TSV for existing non-null data for both old and new prop name.

        Args:
            project_id(str): The project_id of the TSVs.
            node(str): The name of the node TSV to change column names in.
            props(dict): A dict with keys of old prop names to change with values as new names. {'old_prop':'new_prop'}

        Example:
            This changes the column header "time_of_surgery" to "hour_of_surgery" in the surgery TSV.
            change_prop_name(project_id='P001',node='surgery',props={'time_of_surgery':'hour_of_surgery'})
        """

        print("\tAttempting to change prop names in node '{0}': {1}".format(node,props))
        df = self.read_tsv(project_id=project_id,node=node,name=name)

        if df is None:
            print("\t\tNo '{0}' TSV found in project '{1}'. Nothing changed.".format(node,project_id))
            return

        old_prop = list(props)[0]
        new_prop = props[old_prop]

        if old_prop not in df: # old property not in TSV, fail
            if new_prop in df:
                nn = df.loc[df[new_prop].notnull()]
                print("\t\tOld prop name '{}' not found in the '{}' TSV. New prop name '{}' already in TSV with {} non-null records!".format(old_prop,node,new_prop,len(nn)))
            else:
                print("\t\tNeither old prop name '{}' nor new prop name '{}' found in the '{}' TSV. Nothing changed.".format(old_prop,new_prop,node))
            return df

        if new_prop in df:
            nn = df.loc[df[new_prop].notnull()]
            if len(nn) > 0:
                print("\t\tExisting '{0}' data found in TSV: {1} non-null records!".format(new_prop,len(nn)))
                onn = df.loc[df[old_prop].notnull()]
                if len(onn) == 0:
                    print("\t\t\tNo non-null data in old_prop '{}'. Keeping existing data in new_prop '{}'.".format(old_prop,new_prop))
                    df.drop(columns=[old_prop],inplace=True)
                    return df
                else:
                    print("\t\t\t{} non-null old_prop '{}' records found and {} non-null new_prop '{}' records found. \n\n\n\n\nCheck existing '{}' data before running this script!!!\n\n\n\n\n.".format(len(onn),old_prop,len(nn),new_prop,project_id))
                    return df
            else: # if all data is null, drop the column
                df.drop(columns=[new_prop],inplace=True)
                print("\t\tProperty '{0}' already in TSV with all null records. Dropping empty column from TSV.".format(new_prop))

        try:
            nn = df.loc[df[old_prop].notnull()]
            print("\t\tProp name changed from '{}' to '{}' in '{}' TSV with {} non-null records.".format(old_prop,new_prop,node,len(nn)))
            df[new_prop] = df[old_prop]
            df.drop(columns=[old_prop],inplace=True,errors='raise')
            df = self.write_tsv(df,project_id,node,name=name)

        except Exception as e:
            print("\tCouldn't change prop names: {}".format(e))

        return df

    def drop_props(self,project_id,node,props,name='temp',drop_nn=True):
        """
        Function drops the list of props from column headers of a node TSV.
        Args:
            node(str): The node TSV to drop headers from.
            props(list): List of column headers to drop from the TSV.
        Example:
            This will drop the 'military_status' prop from the 'demographic' node.
            drop_props(node='demographic',props=['military_status'])
        """
        if not isinstance(props,list):
            if isinstance(props, str):
                props = [props]
            else:
                print("\tPlease provide props to drop as a list or string:\n\t{}".format(props))

        print("\tAttempting to drop props from '{0}':".format(node))

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        filename = "{}_{}_{}.tsv".format(name,project_id,node)

        if df is None:
            print("\t\tNo '{0}' TSV found in project '{1}'. Nothing changed.".format(node,project_id))
            return

        dropped,not_found,not_dropped,errors = [],[],[],[]

        for prop in props:
            if prop in df:
                nn = df.loc[df[prop].notnull()]
                if not nn.empty:
                    print("\n\nWarning!\n\t\tFound {0} non-null records for '{1}' in '{2}' TSV!".format(len(nn),prop,node))
                    if drop_nn is not True:
                        print("\t\tExisting '{}' data not dropped from '{}' TSV!\n\n\t\tSet 'drop_nn' to True to override this warning.")
                        return df
                try:
                    df = df.drop(columns=[prop],errors='raise')
                    dropped.append(prop)
                except Exception as e:
                    not_dropped.append(prop)
                    errors.append(e)
                    continue
            else:
                not_found.append(prop)

        if len(not_found) > 0:
            print("\t\tWarning: These props were not found in the '{}' TSV: {}".format(node,not_found))

        if len(not_dropped) > 0:
            print("\t\tWarning! Some props were NOT dropped from '{}' TSV: {} {}".format(node,not_dropped,list(set(errors))))

        if len(dropped) > 0:
            print("\t\tprops dropped from '{}' and data written to '{}' TSV: {}".format(node,node,dropped))
            df = self.write_tsv(df,project_id,node)
        else:
            print("\t\tNo props dropped. '{}' TSV unchanged.".format(node))

        return df

    def change_enum(self,project_id,node,prop,enums,name='temp'):
        """
        Changes an enumeration value in the data.
        Args:
            project_id(str): The project_id of the data.
            node(str): The node TSV to change enumeration values in.
            prop(str): The prop (an enum) to change values for.
            enums(dict): A dict containing the mapping of {'old':'new'} enum values.
        Example:
            This changes all 'Percent' to 'Pct' in prop 'test_units' of node 'lab_test'
            change_enum(project_id=project_id,node='lab_test',prop='test_units',enums={'Percent':'Pct'})
        """
        print("\tChanging enum values for '{}' in '{}' TSV: {}".format(prop,node,enums))

        df = self.read_tsv(project_id,node,name)

        if df is None:
            print("\t\tNo '{}' TSV found in project '{}'. No TSVs changed.".format(node,project_id))
            return

        if prop not in df:
            print("\t\t'{}' not found in '{}' TSV! Nothing changed.".format(prop,node))
            return df

        nn = df.loc[df[prop].notnull()]
        if nn.empty:
            print("\t\tAll null '{}' enum values in '{}' TSV! Nothing changed.".format(prop,node))
            return df

        changed,not_changed = [],[]

        for old_enum in list(enums.keys()):

            new_enum = enums[old_enum]
            old_total = len(df.loc[df[prop]==old_enum])

            if old_total == 0:
                print("\t\tNo records found with prop '{}' equal to '{}'; '{}' TSV unchanged. Values include: \n\n{}".format(prop,old_enum,node,df[prop].value_counts()))
                continue

            if new_enum == 'null':
                try:
                    df.at[df[prop]==old_enum,prop] = np.nan
                    changed.append(prop)
                    print("\tChanged {} enum values from '{}' to 'NaN' for prop '{}'".format(old_total,old_enum,prop))
                except Exception as e:
                    not_changed.append(prop)
                    print("\tCouldn't change enum value from '{}' to 'NaN' for prop '{}': {}".format(old_enum,prop,e))
            else:
                try:
                    df.at[df[prop]==old_enum,prop] = new_enum
                    changed.append(prop)
                    new_total = len(df.loc[df[prop]==new_enum])
                    print("\t\tChanged {} enum values from '{}' to '{}' for prop '{}'".format(new_total,old_enum,new_enum,prop))
                except Exception as e:
                    not_changed.append(prop)
                    print("\tCouldn't change enum value '{}' to '{}' for prop '{}': {}".format(old_enum,new_enum,prop,e))

        if len(not_changed) > 0:
            print("\t\tEnum values NOT changed in '{}' TSV: {}".format(node,not_changed))

        if len(changed) > 0:
            self.write_tsv(df,project_id,node,name)

        else:
            print("\t\tNo enum values were changed in '{}' node. No TSVs changed.".format(node))

        return df


    def drop_links(self,project_id,node,links,name='temp'):
        """
        Function drops the list of nodes in 'links' from column headers of a node TSV, including the 'id' and 'submitter_id' for the link.
        Args:
            project_id(str): The project_id of the TSV.
            node(str): The node TSV to drop link headers from.
            links(list): List of node link headers to drop from the TSV.
        Example:
            This will drop the links to 'cases' node from the 'demographic' node.
            drop_links(project_id=project_id,node='demographic',links=['cases'])
        """

        print("\t{}:\n\t\tDropping links to {}".format(node,links))

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        dropped = 0
        for link in links:
            sid = "{}.submitter_id".format(link)
            uuid = "{}.id".format(link)
            if sid in df.columns:
                df = df.drop(columns=[sid])
                dropped += 1
            if uuid in df.columns:
                df = df.drop(columns=[uuid])
                dropped += 1
            count = 1
            sid = "{}.submitter_id#{}".format(link,count)
            while sid in df.columns:
                df = df.drop(columns=[sid])
                dropped += 1
                count += 1
                sid = "{}.submitter_id#{}".format(link,count)
            count = 1
            uuid = "{}.id#{}".format(link,count)
            while uuid in df.columns:
                df = df.drop(columns=[uuid])
                dropped += 1
                count += 1
                uuid = "{}.submitter_id#{}".format(link,count)
        if dropped > 0:
            df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
            print("\tLinks {} dropped from '{}' and TSV written to file: \n\t{}".format(links,node,filename))
        else:
            print("\tNone of {} links found in '{}' TSV.".format(links,node))
        return df

    def batch_drop_links(self,project_id,links):
        """
        Takes a dictionary of nodes and links to drop and drops links from each node's TSV headers. Do after, e.g., batch_add_visits().
        Args:
            project_id(str): The project_id of the TSVs.
            links(dict): A dict of nodes with links to remove, e.g., {'node':['link1','link2']}.
        Example:
            This drops the columns 'cases.submitter_id' and 'cases.id' (and treatments/medications submitter_id and id) from the 'allergy' node TSV and saves it.
            batch_drop_links(project_id=project_id,links={'allergy': ['cases', 'treatments', 'medications']}
        """
        for node in list(links.keys()):
            links_to_drop = links[node]
            df = self.drop_links(project_id=project_id,node=node,links=links_to_drop)

    def merge_links(self,project_id,node,link,links_to_merge,name='temp'):
        """
        Function merges links in 'links_to_merge' into a single 'link' in a 'node' TSV.
        This would be used on a child node after the merge_nodes function was used on a list of its parent nodes.
        Args:
            project_id(str): The project_id of the TSV.
            link(str): The master link to merge links to.
            links_to_merge(list): List of links to merge into link.
        Example:
            This will merge 'imaging_mri_exams' and 'imaging_fmri_exams' into one 'imaging_exams' column.
            merge_links(project_id=project_id,node='imaging_file',link='imaging_exams',links_to_merge=['imaging_mri_exams','imaging_fmri_exams'])
            This fxn is mostly for merging the 7 imaging_exam subtypes into one imaging_exams link for imaging_file node. Not sure what other use cases there may be.
            links_to_merge=['imaging_fmri_exams','imaging_mri_exams','imaging_spect_exams','imaging_ultrasonography_exams','imaging_xray_exams','imaging_ct_exams','imaging_pet_exams']
        """
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        link_name = "{}.submitter_id".format(link)
        df[link_name] = np.nan
        for sublink in links_to_merge:
            sid = "{}.submitter_id".format(sublink)
            df.loc[df[link_name].isnull(), link_name] = df[sid]
        df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
        print("\tLinks merged to '{}' and data written to TSV file: \n\t\t{}".format(link,filename))
        return df

    def drop_ids(self,project_id,node,name='temp'):
        """
        Drops the 'id' column from node TSV.
        Example:
            drop_ids(project_id=project_id,node=node)
        """

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        dropped = False
        if 'id' in df.columns:
            self.drop_props(project_id=project_id,node=node,props=['id'])
            dropped = True
        r = re.compile(".*s\.id")
        ids_to_drop = list(filter(r.match, df.columns))
        if len(ids_to_drop) > 0:
            self.drop_props(project_id=project_id,node=node,props=ids_to_drop)
            dropped = True
        if not dropped:
            print("\t{}:".format(node))
            print("\t\tNo UUID headers found in the TSV.".format(node))
        else:
            print("\t\tAll ids dropped from {}".format(node))

    def batch_drop_ids(self,project_id,suborder,name='temp'):
        """
        Drops the 'id' column from all the TSVs in 'suborder' dictionary obtained by running, e.g.:
        suborder(list of tuples) = get_submission_order(dd,project_id,prefix='temp',suffix='tsv')
        """

        for node_order in suborder:

            node = node_order[0]
            print("\t{}:".format(node))

            df = self.read_tsv(project_id=project_id,node=node,name=name)
            # filename = "{}_{}_{}.tsv".format(name,project_id,node)

            dropped = False
            if 'id' in df.columns:
                self.drop_props(project_id=project_id,node=node,props=['id'])
                dropped = True
            r = re.compile(".*s\.id")
            ids_to_drop = list(filter(r.match, df.columns))

            if len(ids_to_drop) > 0:
                self.drop_props(project_id=project_id,node=node,props=ids_to_drop)
                dropped = True

            if not dropped:
                print("\t{}:".format(node))
                print("\t\tNo UUID headers found in the TSV.".format(node))

    def drop_ids_from_temp(self,project_id,suffix='tsv',name='temp'):

        pattern = "{}*{}".format(name,suffix)
        filenames = glob.glob(pattern)

        for filename in filenames:
            regex = "{}_{}_(.+).{}".format(name,project_id,suffix)
            match = re.search(regex, filename)
            if match:
                node = match.group(1)
                print("\tDropping ids from '{}' node in file '{}'".format(node,filename))
                data = self.drop_ids(project_id=project_id,node=node)
            else:
                print("\tNo node matched filename: '{}'".format(filename))

        return filenames

    def create_project(self,program,project):
        """ Create the program/project:
        """
        project_id = "{}-{}".format(program, project)
        prog_txt = """{{
          "type": "program",
          "dbgap_accession_number": "{}",
          "name": "{}"
        }}""".format(program,program)
        prog_json = json.loads(prog_txt)
        data = self.sub.create_program(json=prog_json)
        print("\t{}".format(data))
        proj_txt = """{{
          "type": "project",
          "code": "{}",
          "dbgap_accession_number": "{}",
          "name": "{}"
        }}""".format(project,project,project)
        proj_json = json.loads(proj_txt)
        data = self.sub.create_project(program=program,json=proj_json)
        print("\t{}".format(data))

    def remove_special_chars(self,project_id,node,name='temp'):
        """ Replace a special character in 'Parkinson's Disease'
        """

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        df_txt = df.to_csv(sep='\t',index=False)

        if 'Â' in df_txt or 'Ã' in df_txt:
            substring = 'Parkins.+?isease'
            df_txt2 = re.sub(substring,"Parkinson's Disease",df_txt)
            df = pd.read_csv(StringIO(df_txt2),sep='\t',dtype=str) # this converts int to float (adds .0 to int)
            df.to_csv(filename,sep='\t',index=False, encoding='utf-8')
            print("\tSpecial chars removed from: {}".format(filename))

        else:
            print("\tNo special chars found in {}".format(filename))

        return df

    def floats_to_integers(self,project_id,node,prop,name='temp'):
        """ Remove trailing zeros ('.0') from integers. """

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        df[prop] = df[prop].str.extract(r'^(\d+).0$', expand=True)
        df.to_csv(filename,sep='\t',index=False, encoding='utf-8')
        print("\tTrailing '.0' decimals removed from: {}".format(filename))
        return df

    def get_submission_order(self,dd,project_id,name='temp',suffix='tsv',missing_nodes=['project','study','case','visit'],check_done=True):
        """
        Gets the submission order for a directory full of TSV data templates.
        Example:
            suborder = stag_mig.get_submission_order(stag_dd,project_id,name='temp',suffix='tsv')
        """

        pattern = "{}*{}".format(name,suffix)
        filenames = glob.glob(pattern)

        if check_done is True:
            if os.path.exists('done'):
                done_files = glob.glob("done/{}".format(pattern))
                filenames += done_files
            else:
                print("\tNo files found in 'done' directory for '{}' project.".format(project_id))

        all_nodes = []
        suborder = {}
        for filename in filenames:
            regex = "{}_{}_(.+).{}".format(name,project_id,suffix)
            match = re.search(regex, filename)
            if match:
                node = match.group(1)
                if node in list(dd):
                    all_nodes.append(node)
                else:
                    print("\tThe node '{}' is not in the data dictionary! Skipping...".format(node))

        print("\tFound the following nodes:\n\t\t{}".format(all_nodes))

        # Check for the common missing root nodes
        for missing_node in missing_nodes:
            if missing_node not in all_nodes and missing_node in list(dd):
                suborder[missing_node]=0

        checked = []
        node = all_nodes.pop(0)
        while len(all_nodes) > 0:

            if node in suborder.keys():
                node = all_nodes.pop(0)

            print("\t\tDetermining order for node '{}'.".format(node)) # for trouble-shooting

            node_links = dd[node]['links']
            for link in node_links: #link = node_links[0]
                if 'subgroup' in list(link):
                    for subgroup in link['subgroup']:#subgroup=link['subgroup'][0]

                        if subgroup['target_type'] == 'project':
                            suborder[node]=1

                        elif subgroup['target_type'] in list(suborder.keys()):
                            suborder[node]=suborder[subgroup['target_type']]+1

                        elif subgroup['target_type'] == 'core_metadata_collection':
                            if node in checked:
                                print("\tNode {} has been checked before.".format(node))
                                suborder[node] = 2
                            else:
                                checked.append(node)
                        elif subgroup['target_type'] in all_nodes:
                            all_nodes.append(node)
                            node = subgroup['target_type']
                            all_nodes.remove(subgroup['target_type'])
                    # if node in list(suborder.keys()):
                    #     continue
                    # else:
                    #     all_nodes.append(node)
                    #     node = all_nodes.pop(0)

                elif 'target_type' in list(link) and link['required'] is True:
                    if link['target_type'] == 'project':
                        suborder[node]=1
                    elif link['target_type'] in list(suborder.keys()):
                        suborder[node]=suborder[link['target_type']]+1
                    elif link['target_type'] in all_nodes:
                        all_nodes.append(node)
                        node = link['target_type']
                        all_nodes.remove(link['target_type'])
                    else: #skip it for now
                        all_nodes.append(node)
                        node = all_nodes.pop(0)

                else:
                    print("\tNo link target_type found for node '{}'".format(node))

        #suborder = sorted(suborder.items(), key=operator.itemgetter(1))
        suborder = {key:val for key, val in suborder.items() if val > 0}
        print("\tSubmission Order: \n\t\t{}".format(suborder))
        return suborder

    def submit_tsvs(self,project_id,suborder,check_done=False,remove_done=False,drop_ids=False,name='temp',chunk_size=1000):
        """
        Submits all the TSVs in 'suborder' dictionary obtained by running, e.g.:
        suborder = stag_mig.get_submission_order(stag_dd,project_id,name='temp',suffix='tsv')
        data = stag_mig.submit_tsvs(project_id,suborder,check_done=False,name='temp')
        """

        logname = "submission_{}_logfile.txt".format(project_id)

        done_cmd = ['mkdir','-p','done']
        failed_cmd = ['mkdir','-p','failed']
        try:
            output = subprocess.check_output(done_cmd, stderr=subprocess.STDOUT).decode('UTF-8')
            output = subprocess.check_output(failed_cmd, stderr=subprocess.STDOUT).decode('UTF-8')
        except Exception as e:
            output = e.output.decode('UTF-8')
            print("ERROR:" + output)

        with open(logname, 'w') as logfile:

            for node in suborder:
                filename="{}_{}_{}.tsv".format(name,project_id,node)
                done_file = Path("done/{}".format(filename))
                failed_file = Path("failed/{}".format(filename))

                if not done_file.is_file() or check_done is False:

                    if drop_ids is True:
                        data = self.drop_ids(project_id=project_id,node=node,name=name)

                    print(str(datetime.datetime.now()))
                    logfile.write(str(datetime.datetime.now()) + " Submitting '{}'.".format(filename))

                    try:
                        data = self.submit_file(project_id=project_id,filename=filename,chunk_size=chunk_size)
                    except Exception as e:
                        print("\tError submitting file: {}".format(e))
                        data = e
                        pass

                    logfile.write("{}\n{}\n\n".format(filename,json.dumps(data))) #put in log file
                    print("Submission log written to file: '{}'.".format(logname))


                    if len(data['invalid']) == 0 and len(data['succeeded']) > 0:
                        mv_done_cmd = ['mv',filename,'done']
                        try:
                            output = subprocess.check_output(mv_done_cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                            print("Submission successful. Moving file to done:\n\t\t{}\n\n".format(filename))
                        except Exception as e:
                            print("\tError moving file to 'done' dir: {}".format(e))
                            pass

                    else:
                        if len(data['invalid'])>0:

                            for i in invalid_records:
                                print("{}".format(data['invalid'][i]))

                        print("Need to fix {} errors in '{}'".format(len(invalid_records),filename))

                        mv_failed_cmd = ['mv',filename,'failed']
                        try:
                            output = subprocess.check_output(mv_failed_cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                            print("Submission failed. Moving file to failed:\n\t\t{}".format(filename))
                        except Exception as e:
                            print("Error moving file to 'failed' dir: {}".format(e))
                            pass

                else:
                    print("\tPreviously submitted file already exists in done directory:\n\t\t{}\n".format(done_file))
                    if remove_done is True:
                        rm_cmd = ['rm',filename]
                        try:
                            output = subprocess.check_output(rm_cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                            print("\t\t'{}' file removed.\n\t\t\t{}".format(name,filename))
                        except Exception as e:
                            output = e.output.decode('UTF-8')
                            print("ERROR:" + output)

    def check_migration_counts(self, projects=None, overwrite=False):
        """ Gets counts and downloads TSVs for all nodes for every project.
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
                print("\t{} records found in node '{}' in project '{}'.".format(str(count),node,project_id))

                if count > 0:
                    filename = str(mydir+'/'+project_id+'_'+node+'.tsv')
                    if (os.path.isfile(filename)) and (overwrite is False):
                        print('\tPreviously downloaded '+ filename )
                    else:
                        prog,proj = project_id.split('-',1)
                        self.sub.export_node(prog,proj,node,'tsv',filename)

        cmd = ['ls',mydir] #look in the download directory
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
        except Exception as e:
            output = 'ERROR:' + e.output.decode('UTF-8')

        return output


    def required_not_reported(self,project_id,node,prop,name='temp',replace_value='Not Reported'):
        """ Change null values for a required prop to "Not Reported".
        """

        print("\t{}:\n\t\tChanging values for prop '{}'".format(node,prop))
        filename = "{}_{}_{}.tsv".format(name,project_id,node)
        df = self.read_tsv(project_id=project_id,node=node,name=name)

        try:
            original_count = len(df.loc[df[prop]==replace_value])
            df[prop].fillna(replace_value, inplace=True)
            new_count = len(df.loc[df[prop]==replace_value])
            replace_count = new_count - original_count
            df = self.write_tsv(df,project_id,node)
            print("\t{} missing (NaN) value(s) for required prop '{}' in '{}' node changed to '{}'.".format(replace_count,prop,node,replace_value))

        except Exception as e:
            print("\tNo TSV found for node '{}': {} {}.".format(node,type(e),e))

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
        results = {
            "invalid": {},  # these are invalid records
            "other": [],  # any unhandled API responses
            "details": [],  # entire API response details
            "succeeded": [],  # list of submitter_ids that were successfully updated/created
            "responses": [],  # list of API response codes
        }

        # Read the file in as a pandas DataFrame
        f = os.path.basename(filename)
        if f.lower().endswith(".csv"):
            df = pd.read_csv(filename, header=0, sep=",", dtype=str).fillna("")
        elif f.lower().endswith(".xlsx"):
            xl = pd.ExcelFile(filename)  # load excel file
            sheet = xl.sheet_names[0]  # sheetname
            df = xl.parse(sheet)  # save sheet as dataframe
            converters = {col: str for col in list(df)}  # make sure int isn't converted to float
            df = pd.read_excel(filename, converters=converters).fillna("")  # remove nan
        elif filename.lower().endswith((".tsv", ".txt")):
            df = pd.read_csv(filename, header=0, sep="\t", dtype=str).fillna("")
        else:
            raise Gen3UserError("Please upload a file in CSV, TSV, or XLSX format.")
        df.rename(columns={c: c.lstrip("*") for c in df.columns}, inplace=True)  # remove any leading asterisks in the DataFrame column names

        # Check uniqueness of submitter_ids:
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            print("\n\n\tWarning: file contains duplicate submitter_ids. \n\tNote: submitter_ids must be unique within a node!\n\n")
            results['invalid'][filename] = 'duplicate submitter_ids in file!'
            return results

        # Chunk the file
        print("\nSubmitting {} with {} records.".format(filename, str(len(df))))
        program, project = project_id.split("-", 1)
        api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
        headers = {"content-type": "text/tab-separated-values"}

        start = row_offset
        end = row_offset + chunk_size
        chunk = df[start:end]

        count = 0

        # Start the chunking loop:
        while (start + len(chunk)) <= len(df):

            timeout = False
            valid_but_failed = []
            invalid = []
            count += 1
            print("Chunk {} (chunk size: {}, submitted: {} of {})".format(str(count),str(chunk_size),str(len(results["succeeded"]) + len(results["invalid"])),str(len(df))))

            try:
                response = requests.put(api_url,auth=self._auth_provider,data=chunk.to_csv(sep="\t", index=False),headers=headers,).text
            except Exception as e:
                results["details"].append(e)
                continue


            if ("Request Timeout" in response or "413 Request Entity Too Large" in response or "Connection aborted." in response or "service failure - try again later" in response):  # time-out, response is not valid JSON at the moment

                print("\t Reducing Chunk Size: {}".format(response))
                results["responses"].append("Reducing Chunk Size: {}".format(response))
                timeout = True

            else:
                try:
                    json_res = json.loads(response)
                except Exception as e:
                    raise Gen3Error("Unable to parse API response as JSON!\n\t{}: {}".format(response,e))

                if "message" in json_res and "code" not in json_res:
                    print("\t No code in the API response for Chunk {}: {}\n\t {}".format(str(count), json_res.get("message"),json_res.get("transactional_errors")))
                    results["responses"].append(
                        "Error Chunk {}: {}".format(str(count), json_res.get("message")))
                    results["other"].append(json_res.get("transactional_errors"))

                elif "code" not in json_res:
                    print("\t Unhandled API-response: {}".format(response))
                    results["responses"].append("Unhandled API response: {}".format(response))

                elif json_res["code"] == 200:
                    # success
                    entities = json_res.get("entities", [])
                    print("\t Succeeded: {} entities.".format(str(len(entities))))
                    results["responses"].append("Chunk {} Succeeded: {} entities.".format(str(count), str(len(entities))))

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        results["succeeded"].append(sid)

                elif (json_res["code"] == 400 or json_res["code"] == 403 or json_res["code"] == 404):
                    # failure
                    entities = json_res.get("entities", [])
                    print("\tChunk Failed: {} entities.".format(str(len(entities))))
                    results["responses"].append("Chunk {} Failed: {} entities.".format(str(count), str(len(entities))))

                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        if entity["valid"]:  # valid but failed
                            valid_but_failed.append(sid)
                        else:  # invalid and failed

                            message = str(entity["errors"])
                            print(message)
                            results["invalid"][sid] = message
                            invalid.append(sid)
                    print("\tInvalid records in this chunk: {}".format(len(invalid)))

                elif json_res["code"] == 500:  # internal server error
                    print("\t Internal Server Error: {}".format(response))
                    results["responses"].append("Internal Server Error: {}".format(response))

            if (len(valid_but_failed) > 0 and len(invalid) > 0):  # if valid entities failed bc grouped with invalid, submission fails, move file to done
                chunk = chunk.loc[df["submitter_id"].isin(valid_but_failed)]  # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
                print("Retrying submission of valid entities from failed chunk: {} valid entities.".format(str(len(chunk))))

            elif (len(valid_but_failed) > 0 and len(invalid) == 0):  # if all entities are valid but submission still failed, probably due to duplicate submitter_ids. Can remove this section once the API response is fixed: https://ctds-planx.atlassian.net/browse/PXP-3065
                raise Gen3Error("Please check your data for correct file encoding, special characters, or duplicate submitter_ids or ids.")

            elif timeout is False:  # get new chunk if didn't timeout
                start += chunk_size
                end = start + chunk_size
                chunk = df[start:end]

            else:  # if timeout, reduce chunk size and retry smaller chunk
                if chunk_size >= 2:
                    chunk_size = int(chunk_size / 2)
                    end = start + chunk_size
                    chunk = df[start:end]
                    print("Retrying Chunk with reduced chunk_size: {}".format(str(chunk_size)))
                    timeout = False
                else:
                    raise Gen3SubmissionError("Submission is timing out. Please contact the Helpdesk.")

        print("Finished data submission.")
        print("Successful records: {}".format(str(len(set(results["succeeded"])))))
        print("Failed invalid records: {}".format(str(len(results["invalid"]))))

        return results


    def change_all_visits(self,project_id,name='temp'):
        """ Change links to visit for every node in a project_tsvs directory.
        """
        # find temp_files with visits.id prop
        print("\tChanging visit links for TSVs in {}".format(project_id))
        grep_cmd = 'grep -rl . -e "visits.id"'
        vfiles = subprocess.check_output(grep_cmd, shell=True).decode("utf-8").split('\n')
        vfiles = [vfile for vfile in vfiles if re.search("^./{}_".format(name),vfile)]

        node_regex = re.compile('^./{}_{}_([a-z0-9_]+)\.tsv'.format(name,project_id))
        nodes = [node_regex.match(vfile).groups()[0] for vfile in vfiles]

        for node in nodes:
            self.change_visit_links(project_id=project_id,node=node,name='temp')


    def change_visit_links(self,project_id,node,name='temp'):
        """ for DM v2.2 change: change visits.submitter_id to visit_id and add links to case
        """

        # read the visit TSV
        vdf = self.read_tsv(project_id=project_id,node='visit')

        if vdf is not None:
            vdf.rename(columns={'submitter_id':'visit_id'},inplace=True)
            v = vdf[['visit_id','cases.submitter_id']]

            ndf = self.read_tsv(project_id=project_id,node=node)
            props = {"visits.submitter_id":"visit_id"}
            try:
                ndf.rename(columns=props,inplace=True)
                n = pd.merge(ndf,v,on='visit_id')
                n.drop(columns=['visits.id'],inplace=True)
                self.write_tsv(df=n,project_id=project_id,node=node,name='temp')
                return n
            except Exception as e:
                print(e)
            ### Add "cases.submitter_id" to TSVs with new "visit_ids"
        else:
            ndf = self.read_tsv(project_id=project_id,node=node)
            n = ndf.drop(columns=['visits.id','visits.submitter_id'])
            self.write_tsv(df=n,project_id=project_id,node=node,name='temp')
            print("\t\tNo visit TSV found in project '{}'; dropping links to visit.".format(project_id))

    def delete_node(self,project_id,node,name='temp'):
        """ Delete a node TSV from a project
        """
        print("\tAttempting to delete TSV for node '{}' in project '{}'.".format(node,project_id))
        filename = "{0}_{1}_{2}.tsv".format(name,project_id,node)
        if os.path.isfile(filename):
            try:
                os.remove(filename)
                print("\t\tTSV deleted: '{}'".format(filename))
            except Exception as e:
                print("\t\tCouldn't delete '{}': {}".format(filename,e))
        else:
            print("\t\tNo TSV found: '{}'".format(filename))


    def add_case_ids(self,project_id,dd,nodes='all',name='temp'):
        """ Add case_ids to a list of nodes

            Usage:
            mig.add_case_ids(project_id=project_id, dd=prep_dd)
        """

        pattern = '{}_{}_*.tsv'.format(name,project_id)
        tsvs = glob.glob(pattern)

        node_regex = re.compile('^{}_{}_([a-z0-9_]+)\.tsv'.format(name,project_id))
        all_nodes = [node_regex.match(tsv).groups()[0] for tsv in tsvs]

        if nodes == 'all':
            nodes = all_nodes
        elif isinstance(nodes,str):
            nodes = [nodes]

        if 'case' not in nodes:
            print("\t\tNo 'case' TSV found in project '{}': {}".format(project_id,nodes))
            return

        print("\tAdding case_ids to nodes in project '{}':\n\t\t{}".format(project_id,nodes))
        for node in nodes:
            print("\t{}".format(node.upper()))
            df = self.read_tsv(project_id,node)

            if 'case_submitter_id' in df:
                csi = df.loc[df['case_submitter_id'].notnull()]
                if csi.empty:
                    df.drop(columns=['case_submitter_id'],inplace=True)
                    print("\t\tDropped 'case_submitter_id' from '{}' TSV; all null values.".format(node))
                else:
                    print("\t\tFound '{}' records in {} with non-null case_submitter_id.".format(len(csi),node))
                    df.rename(columns={'case_submitter_id':'case_ids'},inplace=True)
                    self.write_tsv(df,project_id,node)

            links = self.find_df_links(df=df,dd=dd)

            if node == 'case':
                df['case_ids'] = df['submitter_id']
                try:
                    print("\t\tAdded case_ids to 'case' node.")
                    self.write_tsv(df,project_id,node)
                except Exception as e:
                    print("\t\tCouldn't add case_ids to 'case' node.")

            elif 'case' in links:
                df['case_ids'] = df['cases.submitter_id']
                try:
                    print("\t\tAdded 'case_ids' to '{}' TSV in project '{}'".format(node,project_id))
                    self.write_tsv(df,project_id,node)
                except Exception as e:
                    print("\t\tCouldn't add case_ids to '{}' node.".format(node))

            elif 'project' in links:
                cdf = self.read_tsv(project_id,'case')
                if cdf is not None:
                    cids = list(set(cdf.submitter_id))
                    case_ids = ','.join(cids)
                    df['case_ids'] = case_ids
                    try:
                        print("\t\tAdded 'case_ids' to '{}' TSV in project '{}'".format(node,project_id))
                        self.write_tsv(df,project_id,node)
                    except Exception as e:
                        print("\t\tCouldn't add case_ids to '{}'".format(node))
                else:
                    print("\t\tNo 'case' TSV found in project '{}'".format(project_id))
            else:
                while 'case' not in links:
                    #print("\t{}:".format(node))
                    if len(links) > 1:
                        print("\n\n\nLinks greater than one! '{}'".format(node))

                    for l in links:
                        ldf = self.read_tsv(project_id,l)
                        ldf['{}.submitter_id'.format(links[l])] = ldf['submitter_id']

                        if 'cases.submitter_id' in ldf:
                            df = pd.merge(df,ldf[['{}.submitter_id'.format(links[l]),'cases.submitter_id']],on='{}.submitter_id'.format(links[l]))
                            df.rename(columns={'cases.submitter_id':'case_ids'},inplace=True)
                            try:
                                print("\t\tAdded case_ids to '{}' TSV.".format(node))
                                self.write_tsv(df,project_id,node)
                            except Exception as e:
                                print("\t\tCouldn't add case_ids to '{}'".format(node))
                        else:
                            print("\n\n\nDidn't find link to case in '{}' TSV!".format(l))

                    links = self.find_df_links(df=ldf,dd=dd)

        return nodes

    def find_df_links(self,df,dd,name='temp'):
        """ Get a list of valid links given a data dictionary and a list of
        """

        node = list(set(df['type']))[0]
        project_id = list(set(df['project_id']))[0]

        link_regex = re.compile('(.+)\.submitter_id')
        df_links = [link_regex.match(header).groups()[0] for header in list(df) if link_regex.match(header) is not None]
        # don't include empty links
        df_links = [df_link for df_link in df_links if not df.loc[df['{}.submitter_id'.format(df_link)].notnull()].empty]

        if 'projects.code' in df:
            df_links += ['projects']

        pattern = '{}_{}_*.tsv'.format(name,project_id)
        tsvs = glob.glob(pattern)
        node_regex = re.compile('^{}_{}_([a-z0-9_]+)\.tsv'.format(name,project_id))
        nodes = [node_regex.match(tsv).groups()[0] for tsv in tsvs]

        links = {}
        for link_def in dd[node]['links']:
            #link_def = dd[node]['links'][0]
            if 'target_type' in link_def:
                link_node = link_def['target_type']
                link_backref = link_def['name']
                if link_node in nodes and link_backref in df_links or link_node == 'project':
                    links[link_node] = link_backref

            elif 'subgroup' in link_def:
                link_nodes = [link['target_type'] for link in link_def['subgroup'] if link['target_type'] in nodes]
                link_backrefs = [link['name'] for link in link_def['subgroup']]
                links = dict(zip(link_nodes,link_backrefs))

            else:
                print("Can't find link '{}.submitter_id' or '{}.submitter_id#1' in '{}' TSV: {}".format(link_def))

        return links


    def move_strings_to_other(self,project_id,node,props,name='temp'):
        """ Move strings from numeric properties into a different string property.
            Not useful in most situations, this script is for legacy data where a "mixed type" of both integers and strings were allowed.

            Arguments:
                project_id (str): The project_id
                node (str): the node with the props.
                props (dict): props = {"num_prop":"str_prop"} where num_prop is the prop with mixed type data that is now numeric only and str_prop is the prop for the string data to be moved into.
        """

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        num_prop = list(props)[0]
        str_prop = props[num_prop]

        nn = df.loc[df[num_prop].notnull()]

        if nn.empty:
            print("\t\tproperty '{}' has all null values in '{}' TSV of project '{}'!".format(num_prop,node,project_id))

        elif num_prop in list(nn):
            str_data = nn.loc[~nn[num_prop].astype(str).str.replace('.', '', regex=False).str.isdigit()]
            num_data = nn.loc[nn[num_prop].astype(str).str.replace('.', '', regex=False).str.isdigit()]

            # move the strings from num_prop to str_prop
            nn.loc[~nn[num_prop].astype(str).str.replace('.', '', regex=False).str.isdigit(), str_prop] = nn[num_prop]
            #nn.loc[~nn[num_prop].str.isnumeric(), str_prop] = nn[num_prop]

            # null the strings in the numeric prop
            nn.loc[~nn[num_prop].astype(str).str.replace('.', '', regex=False).str.isdigit(), num_prop] = np.nan
            #nn.loc[~nn[num_prop].str.isnumeric(), num_prop] = np.nan

            df[num_prop] = nn[num_prop]
            df[str_prop] = nn[str_prop]

            df = self.write_tsv(df,project_id,node,name=name)
            print("\t\t{} numeric value(s) kept in prop '{}'; {} string values moved to prop '{}' in '{}' TSV.".format(len(num_data),num_prop,len(str_data),str_prop,node))

        else:
            print("\t\tproperty '{}' not found in '{}' TSV of project '{}'!".format(num_prop,node,project_id))

        return df


    def drop_empty_links(self,project_id,node,dd,remove_not_required=True,remove_empty=False,name='temp'):
        """
        Drops the 'links.id' and 'links.submitter_id' column from node TSV, where 'links' is the backref of the parent node(s) if the links are all null.

        Example:
            drop_all_links(project_id=project_id,node=node)
        """

        print("\tDropping all links in '{}' TSV.".format(node))

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        link_regex = re.compile('(.+)\.submitter_id')
        df_links = [link_regex.match(header).groups()[0] for header in list(df) if link_regex.match(header) is not None]

        remove_links = []

        # remove not required links:
        if remove_not_required is True:
            not_required_links = [df_link for df_link in df_links if df_link not in dd[node]['required']]
            remove_links += not_required_links

        # remove empty links (default is False)
        if remove_empty is True:
            empty_links = [df_link for df_link in df_links if not df.loc[df['{}.submitter_id'.format(df_link)].isnull()].empty]
            remove_links += empty_links

        remove_links = list(set(remove_links))
        dropped = 0

        for link in remove_links:
            sid = "{}.submitter_id".format(link)
            uuid = "{}.id".format(link)
            if sid in df.columns:
                df = df.drop(columns=[sid])
                dropped += 1
            if uuid in df.columns:
                df = df.drop(columns=[uuid])
                dropped += 1
            count = 1
            sid = "{}.submitter_id#{}".format(link,count)
            while sid in df.columns:
                df = df.drop(columns=[sid])
                dropped += 1
                count += 1
                sid = "{}.submitter_id#{}".format(link,count)
            count = 1
            uuid = "{}.id#{}".format(link,count)
            while uuid in df.columns:
                df = df.drop(columns=[uuid])
                dropped += 1
                count += 1
                uuid = "{}.submitter_id#{}".format(link,count)
        if dropped > 0:
            df = self.write_tsv(df,project_id,node,name=name)
            print("\tLinks {} dropped from '{}' TSV.".format(remove_links,node))
        else:
            print("\tNone of {} links found in '{}' TSV.".format(remove_links,node))
        return df

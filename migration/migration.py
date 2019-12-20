import os, os.path, sys, subprocess, glob, json, re, operator, requests, datetime
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
#from gen3.submission import Gen3Submission

sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/gen3')
from auth import Gen3Auth
from submission import Gen3Submission

sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion')
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
        self.exp = Gen3Expansion(endpoint, auth_provider)

    def read_tsv(self,project_id,node,name='temp'):
        if name is not None:
            filename = "{}_{}_{}.tsv".format(name,project_id,node)
        else:
            filename = "{}_{}.tsv".format(project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo '{}' TSV found.".format(filename))
            return
        return df

    def write_tsv(self,df,project_id,node,name='temp'):
        if name is not None:
            outname = "{}_{}_{}.tsv".format(name,project_id,node)
        else:
            outname = "{}_{}.tsv".format(project_id,node)
        try:
            df.to_csv(outname, sep='\t', index=False, encoding='utf-8')
            print("\tTotal of {} records written to node '{}' in file:\n\t\t{}.".format(len(df),node,outname))
        except Exception as e:
            print("Error writing TSV file: {}".format(e))
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
            pattern = "{}*{}".format(prefix,suffix)
            filenames = glob.glob(pattern)

        elif isinstance(nodes, list):
            filenames = []
            for node in nodes:
                pattern = "{}*{}.{}".format(prefix,node,suffix)
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
                print("\tCopying file {} to:\n\t{}".format(filename,temp_name))
                copyfile(filename,temp_name)
            print("\tTotal of {} '{}' files created.".format(len(filenames),name))

        pattern = "{}*{}".format(name,suffix)
        tempfiles = glob.glob(pattern)
        print("\tReturning list of {} '{}' files found in this directory.".format(len(tempfiles),name))
        return tempfiles

    def merge_nodes(self,project_id,in_nodes,out_node,name='temp'):
        """
        Merges a list of node TSVs into a single TSV.
        Args:
            in_nodes(list): List of node TSVs to merge into a single TSV.
            out_node(str): The name of the new merged TSV.
        """
        print("Merging nodes {} to '{}'.".format(in_nodes,out_node))
        dfs = []
        for node in in_nodes:
            filename = "{}_{}_{}.tsv".format(name,project_id, node)
            try:
                df1 = pd.read_csv(filename,sep='\t', header=0, dtype=str)
                dfs.append(df1)
                print("\t{} node found with {} records.".format(node,len(df1)))
            except IOError as e:
                print("\tCan't read file {}".format(filename))
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

    def merge_properties(self,project_id,node,properties,name='temp'):
        """
        This function merges a list of properties into a single property and then drops the list of properties from the column headers.
        Args:
            project_id(str): The project_id of the data.
            node(str): The node TSV to merge properties in.
            properties(dict): A dictionary of "single_property_to_merge_into":["list","of","properties","to","merge","and","drop"]
        """
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        dropped = []
        for prop in list(properties.keys()):
            if prop not in list(df):
                df[prop] = np.nan
            old_props = properties[prop]
            for old_prop in old_props:
                if old_prop in list(df):
                    df_old = df.loc[df[old_prop].notnull()]
                    df_old[prop] = df_old[old_prop]
                    df_rest = df.loc[df[old_prop].isnull()]
                    df_merged = pd.concat([df_rest,df_old],ignore_index=True,sort=False)
                    df = df_merged.drop(columns=[old_prop])
                    dropped.append(old_prop)
                    print("Property '{}' merged into '{}' and dropped from '{}' TSV.".format(old_prop,prop,node))
                else:
                    print("Property '{}' not found in '{}' TSV. Skipping...".format(old_prop,node))
        if len(dropped) > 0:
            print("Properties {} merged into {}.".format(dropped,list(properties.keys())))
            df = self.write_tsv(df,project_id,node)
            return df
        else:
            print("\tNo properties dropped from '{}'. No TSV written.".format(node))
            return

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

    def create_missing_links(self,project_id,node,link,old_parent,properties,new_dd,old_dd,links=None,name='temp'):
        """
        This fxn creates links TSV for links in a node that don't exist.
        Args:
            node(str): This is the node TSV in which to look for links that don't exist.
            link(str): This is the node to create the link records in.
            old_parent(str): This is the backref of the parent node of 'node' prior to the dictionary change.
            properties(dict): Dict of required properties/values to add to new link records.
        Example:
            This will create visit records that don't exist in the visit TSV but are in the imaging_exam TSV.
            create_missing_links(node='imaging_exam',link='visit',old_parent='cases',properties={'visit_label':'Imaging','visit_method':'In-person Visit'},new_dd=dd,old_dd=prod_dd,links=None)
            create_missing_links(node='diagnosis',link='visit',old_parent='cases',properties={'visit_label':'Unknown','visit_method':'Unknown'},new_dd=dd,old_dd=prod_dd)
        """
        print("Creating missing '{}' records with links to '{}' for '{}'.".format(link,old_parent,node))

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
                print("\tCreating {} records in '{}' with links to same cases as '{}' for missing '{}' links.".format(len(missing),link,old_parent,node))
            else:
                print("\tAll {} records in '{}' node have existing links to '{}'. No new records added.".format(len(df),node,link))
                return link_df.loc[link_df['submitter_id'].isin(link_names)]
        except FileNotFoundError as e:
            link_df = pd.DataFrame()
            print("\tNo '{}' TSV found. Creating new TSV for links.".format(link))
            missing = link_names
        parent_link = "{}.submitter_id".format(old_parent)
        if parent_link in list(df):
            new_links = df.loc[df[link_name].isin(missing)][[link_name,parent_link]]
        else:
            parent_link = "{}.submitter_id#1".format(old_parent)
            new_links = df.loc[df[link_name].isin(missing)][[link_name,parent_link]]
        new_links = new_links.rename(columns={link_name:'submitter_id'})
        new_links['type'] = link
        for prop in properties:
            new_links[prop] = properties[prop]
        if old_parent is not 'cases':
            old_links = old_dd[node]['links']
            for old_link in old_links:
                if old_link['name'] == old_parent:
                    old_node = old_link['target_type']
            old_name = "{}_{}_{}.tsv".format(name,project_id,old_node)
            try:
                odf = pd.read_csv(old_name,sep='\t',header=0,dtype=str)
            except FileNotFoundError as e:
                print("\tNo existing '{}' TSV found. Skipping...".format(node))
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
                except FileNotFoundError as e:
                    print("\tNo existing '{}' TSV found. Skipping...".format(node))
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
            print("{}: links {}, dropping {}".format(node,targets,links_to_drop))
            if 'cases' not in links_to_drop and len(links_to_drop) == 1 and 'visit' in targets and len(targets) == 1:
                df = self.add_missing_links(project_id=project_id,node=node,link='visit')
                if df is not None:
                    df = self.create_missing_links(project_id=project_id,node=node,link='visit',old_parent=links_to_drop[0],properties=required_props,new_dd=new_dd,old_dd=old_dd,links=links)
                    dfs.append(df)
                    total += len(df)
            elif 'cases' in links_to_drop and 'visit' in targets and len(targets) == 1:
                df = self.add_missing_links(project_id=project_id,node=node,link='visit')
                if df is not None:
                    df = self.create_missing_links(project_id=project_id,node=node,link='visit',old_parent='cases',properties=required_props,new_dd=new_dd,old_dd=old_dd,links=links)
                    dfs.append(df)
                    total += len(df)
            else:
                print("\tNo links to 'case' found in the '{}' TSV.".format(node))
        if len(dfs) > 0:
                df = pd.concat(dfs,ignore_index=True,sort=False)
        print("Total of {} missing visit links created for this batch.".format(total))
        return df

    def move_properties(self,project_id,from_node,to_node,properties,dd,parent_node=None,required_props=None,name='temp'):
        """
        This function takes a node with properties to be moved (from_node) and moves those properties/data to a new node (to_node).
        Fxn also checks whether the data for properties to be moved actually has non-null data. If all data are null, no new records are created.
        Args:
            from_node(str): Node TSV to copy data from.
            to_node(str): Node TSV to add copied data to.
            properties(list): List of column headers containing data to copy.
            parent_node(str): The parent node that links the from_node to the to_node, e.g., 'visit' or 'case'.
            required_props(dict): If the to_node has additional required properties, enter the value all records should get for each key.
        Example:
            This moves the property 'military_status' from 'demographic' node to 'military_history' node, which should link to the same parent node 'case'.
            move_properties(from_node='demographic',to_node='military_history',properties=['military_status'],parent_node='case')
        """
        print("Moving {} from '{}' to '{}'.".format(properties,from_node,to_node))

        from_name = "{}_{}_{}.tsv".format(name,project_id,from_node) #from imaging_exam
        try:
            df_from = pd.read_csv(from_name,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo '{}' TSV found with the data to be moved. Nothing to do. Finished.".format(from_node))
            return

        to_name = "{}_{}_{}.tsv".format(name,project_id,to_node) #to reproductive_health
        try:
            df_to = pd.read_csv(to_name,sep='\t',header=0,dtype=str)
            new_file = False
        except FileNotFoundError as e:
            df_to = pd.DataFrame(columns=['submitter_id'])
            print("\tNo '{}' TSV found. Creating new TSV for data to be moved.".format(to_node))
            new_file = True

        # Check that the data to move is not entirely null. If it is, then give warning and quit.
        proceed = False
        exists = False

        for prop in properties:
            if len(df_from.loc[df_from[prop].notnull()]) > 0:
                proceed = True
            if prop in list(df_to.columns):
                exists = True

        if not proceed:
            print("\tNo non-null '{}' data found in '{}' records. No TSVs changed.".format(to_node,from_node))
            return

        if exists:
            print("\tProperties {} already exist in '{}' node.".format(properties,to_node))
            return

        if parent_node is not None:
            parent_link = "{}s.submitter_id".format(parent_node)
            from_no_link = df_from.loc[df_from[parent_link].isnull()] # from_node records with no link to parent_node
            if not from_no_link.empty: # if there are records with no links to parent node
                print("\tWarning: there are {} '{}' records with no links to parent '{}' node!".format(len(from_no_link),from_node,parent_node))
                return
        else:
            parent_link = "{}s.submitter_id".format(to_node)
            # note this only works if the backref is not
        # keep records only if they have some non-null value in "properties"
        all_props = [parent_link] + properties
        new_to = df_from[all_props] #demo_case = demo[['cases.submitter_id']+static_case]
        new_to = new_to[all_props].dropna(thresh=2) # drops any rows where there aren't at least 2 non-null values (1 of them is submitter_id)

        if to_node is 'case':
            new_to.rename(columns={parent_link:'submitter_id'},inplace=True)
            headers = list(new_to)
            case_data = pd.DataFrame(columns=headers)
            case_ids = list(new_to['submitter_id'].unique())
            case_data['submitter_id'] = case_ids
            count = 1
            for case_id in case_ids:
                print("\tGathering unique data for case '{}' ({}/{})".format(case_id,count,len(case_ids)))
                df1 = new_to.loc[new_to['submitter_id']==case_id]
                for header in headers:
                    vals = list(set(df1.loc[df1[header].notnull()][header].unique()))
                    if len(vals) == 1:
                        case_data.loc[case_data['submitter_id']==case_id,header] = vals
                    elif len(vals) > 1:
                        print("{}: {}".format(header,vals))
                        if header == 'age_at_enrollment': # special case hard-coded for BRAIN Commons migration
                            lowest_val = min(vals, key=float)
                            print("Selecting lowest value '{}' from {}.".format(lowest_val,vals))
                            case_data.loc[case_data['submitter_id']==case_id,header] = lowest_val
                count += 1
            all_to = pd.merge(df_to,case_data,on='submitter_id', how='left')
        else:
            new_to['type'] = to_node
            new_to['project_id'] = project_id
            new_to['submitter_id'] = df_from['submitter_id'] + "_{}".format(to_node)
            #only write new_to records if submitter_ids don't already exist in df_to:
            add_to = new_to.loc[~new_to['submitter_id'].isin(list(df_to.submitter_id))]
            all_to = pd.concat([df_to,add_to],ignore_index=True,sort=False)

        # add any missing required properties to new DF
        to_required = list(set(list(dd[to_node]['required'])).difference(list(all_to)))
        link_target = None
        for link in list(dd[to_node]['links']):
            if link['name'] in to_required:
                link_target = link['target_type']
                to_required.remove(link['name'])

        if required_props is not None:
            for prop in to_required:
                if prop in list(required_props.keys()):
                    all_to[prop] = required_props[prop]
                    print("Missing required property '{}' added to new '{}' TSV with all {} values.".format(prop,to_node,required_props[prop]))
                else:
                    all_to[prop] = np.nan
                    print("Missing required property '{}' added to new '{}' TSV with all null values.".format(prop,to_node))

        all_to.to_csv(to_name,sep='\t',index=False,encoding='utf-8')
        print("\tProperties moved to '{}' node from '{}'. Data saved in file:\n\t{}".format(to_node,from_node,to_name))
        return all_to

    def add_property(self,project_id,node,properties):

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        for prop in list(properties.keys()):
            if prop not in list(df):
                df[prop] = properties[prop]
            else:
                print("Property '{}' already in the TSV for node '{}'.".format(prop,node))

        df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
        return df


    def change_property_names(self,project_id,node,properties,name='temp'):
        """
        Changes the names of columns in a TSV.
        Args:
            project_id(str): The project_id of the TSVs.
            node(str): The name of the node TSV to change column names in.
            properties(dict): A dict with keys of old prop names to change with values as new names.
        Example:
            This changes the column header "time_of_surgery" to "hour_of_surgery" in the surgery TSV.
            change_property_names(project_id='P001',node='surgery',properties={'time_of_surgery':'hour_of_surgery'})
        """
        print("\tChanging property names in {} node:\n\t\t{}".format(node,properties))
        df = self.read_tsv(project_id=project_id,node=node,name=name)
        filename = "{}_{}_{}.tsv".format(name,project_id,node)

        try:
            df.rename(columns=properties,inplace = True)
            df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
            print("\tProperties names changed and TSV written to file: \n\t\t{}".format(filename))
            return df
        except Exception as e:
            print("\tCouldn't change property names: {}".format(e))
            return

    def drop_properties(self,project_id,node,properties,name='temp'):
        """
        Function drops the list of properties from column headers of a node TSV.
        Args:
            node(str): The node TSV to drop headers from.
            properties(list): List of column headers to drop from the TSV.
        Example:
            This will drop the 'military_status' property from the 'demographic' node.
            drop_properties(node='demographic',properties=['military_status'])
        """
        print("{}:\n\tDropping {}.".format(node,properties))

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        dropped = []
        for prop in properties:
            try:
                df = df.drop(columns=[prop])
                dropped.append(prop)
            except Exception as e:
                print("\tCouldn't drop property '{}' from '{}':\n\t{}".format(prop,node,e))
                continue
        if len(dropped) > 0:
            df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
            print("\tProperties {} dropped from '{}' and data written to TSV:\n\t{}".format(dropped,node,filename))
            return df
        else:
            print("\tNo properties dropped from '{}' No TSV written.".format(node))
            return

    def change_enum(self,project_id,node,prop,enums,name='temp'):
        """
        Changes an enumeration value in the data.
        Args:
            project_id(str): The project_id of the data.
            node(str): The node TSV to change enumeration values in.
            prop(str): The property (an enum) to change values for.
            enums(dict): A dict containing the mapping of {'old':'new'} enum values.
        Example:
            This changes all 'Percent' to 'Pct' in property 'test_units' of node 'lab_test'
            change_enum(project_id=project_id,node='lab_test',prop='test_units',enums={'Percent':'Pct'})
        """
        print("{}:\n\tChanging values for property '{}'".format(node,prop))
        filename = "{}_{}_{}.tsv".format(name,project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
            success = 0
            for key in list(enums.keys()):
                value = enums[key]
                total = len(df.loc[df[prop]==key])
                if total == 0:
                    print("\tNo records found with property '{}' equal to '{}'. Values in TSV include:\n\t\t{}".format(prop,key,set(list(df[prop]))))
                    continue
                if value == 'null':
                    try:
                        df.at[df[prop]==key,prop] = np.nan
                        success += 1
                        print("\tChanged {} enum values from '{}' to 'NaN' for property '{}'".format(total,key,prop))
                    except Exception as e:
                        print("\tCouldn't change enum value from '{}' to 'NaN' for property '{}'".format(key,prop))
                else:
                    try:
                        df.at[df[prop]==key,prop] = value
                        success += 1
                        print("\tChanged {} enum values from '{}' to '{}' for property '{}'".format(total,key,value,prop))
                    except Exception as e:
                        print("\tCouldn't change enum value '{}' to '{}' for property '{}'".format(key,value,prop))
            if success > 0:
                df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
                print("\tEnum values changed in '{}' node and TSV written to file: \n\t\t{}".format(node,filename))
            else:
                print("\tNo enum values were changed in '{}' node. No TSVs changed.".format(node))
            return df
        except FileNotFoundError as e:
            print("\tNo TSV found for node '{}'.".format(node))

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

        print("{}:\n\tDropping links to {}".format(node,links))

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
        print("Links merged to '{}' and data written to TSV file: \n\t{}".format(link,filename))
        return df

    def drop_ids(self,project_id,node,name='temp'):
        """
        Drops the 'id' column from node TSV.
        """

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        dropped = False
        if 'id' in df.columns:
            self.drop_properties(project_id=project_id,node=node,properties=['id'])
            dropped = True
        r = re.compile(".*s\.id")
        ids_to_drop = list(filter(r.match, df.columns))
        if len(ids_to_drop) > 0:
            self.drop_properties(project_id=project_id,node=node,properties=ids_to_drop)
            dropped = True
        if not dropped:
            print("{}:".format(node))
            print("\tNo UUID headers found in the TSV.".format(node))
        else:
            print("\tAll ids dropped from {}".format(node))

    def batch_drop_ids(self,project_id,suborder,name='temp'):
        """
        Drops the 'id' column from all the TSVs in 'suborder' dictionary obtained by running, e.g.:
        suborder(list of tuples) = get_submission_order(dd,project_id,prefix='temp',suffix='tsv')
        """

        for node_order in suborder:

            node = node_order[0]
            print(node)

            df = self.read_tsv(project_id=project_id,node=node,name=name)
            # filename = "{}_{}_{}.tsv".format(name,project_id,node)

            dropped = False
            if 'id' in df.columns:
                self.drop_properties(project_id=project_id,node=node,properties=['id'])
                dropped = True
            r = re.compile(".*s\.id")
            ids_to_drop = list(filter(r.match, df.columns))

            if len(ids_to_drop) > 0:
                self.drop_properties(project_id=project_id,node=node,properties=ids_to_drop)
                dropped = True

            if not dropped:
                print("{}:".format(node))
                print("\tNo UUID headers found in the TSV.".format(node))

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
        print(data)
        proj_txt = """{{
          "type": "project",
          "code": "{}",
          "dbgap_accession_number": "{}",
          "name": "{}"
        }}""".format(project,project,project)
        proj_json = json.loads(proj_txt)
        data = self.sub.create_project(program=program,json=proj_json)
        print(data)

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
            print("Special chars removed from: {}".format(filename))

        else:
            print("No special chars found in {}".format(filename))

        return df

    def floats_to_integers(self,project_id,node,prop,name='temp'):
        """ Remove trailing zeros ('.0') from integers. """

        df = self.read_tsv(project_id=project_id,node=node,name=name)
        # filename = "{}_{}_{}.tsv".format(name,project_id,node)

        df[prop] = df[prop].str.extract(r'^(\d+).0$', expand=True)
        df.to_csv(filename,sep='\t',index=False, encoding='utf-8')
        print("Trailing '.0' decimals removed from: {}".format(filename))
        return df

    def get_submission_order(self,dd,project_id,name='temp',suffix='tsv',missing_nodes=['project','study','case','visit']):
        """
        Gets the submission order for a directory full of TSV data templates.
        Example:
            suborder = stag_mig.get_submission_order(stag_dd,project_id,name='temp',suffix='tsv')
        """

        pattern = "{}*{}".format(name,suffix)
        filenames = glob.glob(pattern)

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

        print("\tFound the following nodes:\n\t{}".format(all_nodes))

        # Check for the common missing root nodes
        for missing_node in missing_nodes:
            if missing_node not in all_nodes:
                suborder[missing_node]=0

        checked = []
        while len(all_nodes) > 0:

            node = all_nodes.pop(0)
            print("\tDetermining order for node '{}'.".format(node))

            node_links = dd[node]['links']
            for link in node_links:
                if 'subgroup' in list(link):
                    for subgroup in link['subgroup']:

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
                    if node in list(suborder.keys()):
                        continue
                    else:
                        all_nodes.append(node)
                elif 'target_type' in list(link):
                    if link['target_type'] == 'project':
                        suborder[node]=1
                    elif link['target_type'] in list(suborder.keys()):
                        suborder[node]=suborder[link['target_type']]+1
                    else: #skip it for now
                        all_nodes.append(node)
                else:
                    print("No link target_type found for node '{}'".format(node))
        #suborder = sorted(suborder.items(), key=operator.itemgetter(1))
        suborder = {key:val for key, val in suborder.items() if val > 0}
        print("\tSubmission Order: \n\t\t{}".format(suborder))
        return suborder

    def submit_tsvs(self,project_id,suborder,check_done=False,name='temp'):
        """
        Submits all the TSVs in 'suborder' dictionary obtained by running, e.g.:
        suborder = stag_mig.get_submission_order(stag_dd,project_id,name='temp',suffix='tsv')
        data = stag_mig.submit_tsvs(project_id,suborder,check_done=False,name='temp')
        """

        logname = "submission_{}_logfile.txt".format(project_id)
        cmd = ['mkdir','-p','done']
        cmd = ['mkdir','-p','failed']
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
        except Exception as e:
            output = e.output.decode('UTF-8')
            print("ERROR:" + output)
        with open(logname, 'w') as logfile:
            for node in suborder:
                filename="{}_{}_{}.tsv".format(name,project_id,node)
                done_file = Path("done/{}".format(filename))
                failed_file = Path("failed/{}".format(filename))
                if not done_file.is_file() or check_done is False:
                    try:
                        print(str(datetime.datetime.now()))
                        logfile.write(str(datetime.datetime.now()))
                        data = self.sub.submit_file(project_id=project_id,filename=filename,chunk_size=1000)
                        #print("data: {}".format(data)) #for trouble-shooting
                        logfile.write(filename + '\n' + json.dumps(data)+'\n\n') #put in log file
                        if len(data['invalid']) == 0 and len(data['succeeded']) > 0:
                            cmd = ['mv',filename,'done']
                            try:
                                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                                print("Submission successful. Moving file to done:\n\t{}\n\n".format(filename))
                            except Exception as e:
                                output = e.output.decode('UTF-8')
                                print("ERROR:" + output)
                        else:
                            if len(data['invalid'])>0:
                                invalid_records = list(data['invalid'].keys())[0:10]
                                for i in invalid_records:
                                    print(data['invalid'][i])
                            print("Need to fix errors in {}".format(filename))
                            cmd = ['mv',filename,'failed']
                            try:
                                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('UTF-8')
                                print("Submission successful. Moving file to done:\n\t{}\n\n".format(filename))
                            except Exception as e:
                                output = e.output.decode('UTF-8')
                                print("ERROR:" + output)

                    except Exception as e:
                        print(e)
                else:
                    print("\nPreviously submitted file already exists in done directory:\n\t{}\n".format(done_file))

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

    def add_case_submitter_id():

        temp_files = self.make_temp_files(prefix,suffix,name='temp',overwrite=False)
        for temp_file in temp_files:
            df = self.read_tsv(temp_file)

        return

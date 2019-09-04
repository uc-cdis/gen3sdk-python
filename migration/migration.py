import os, os.path, sys, subprocess, glob, json, re, operator, requests
import fnmatch, sys, ntpath, copy
from shutil import copyfile
import numpy as np
from collections import Counter
from statistics import mean
import pandas as pd
from pandas.io.json import json_normalize
# turn off pandas chained assignment warning
pd.options.mode.chained_assignment = None

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File
sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion')
from expansion import Gen3Expansion


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
        # self.sub.submit_file()
        # api = self._endpoint
        # auth = self._auth_provider

    def make_temp_files(self,prefix,suffix,name='temp'):
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
        pattern = "{}*{}".format(prefix,suffix)
        filenames = glob.glob(pattern)
        for filename in filenames:
            temp_name = "temp_{}".format(filename)
            print("Copying file {} to:\n\t{}".format(filename,temp_name))
            copyfile(filename,temp_name)
        print("Total of {} {} files created.".format(len(filenames),name))

    def merge_nodes(self,project_id,in_nodes,out_node):
        """
        Merges a list of node TSVs into a single TSV.
        Args:
            in_nodes(list): List of node TSVs to merge into a single TSV.
            out_node(str): The name of the new merged TSV.
        """
        print("Merging nodes {} to '{}'.".format(in_nodes,out_node))
        dfs = []
        for node in in_nodes:
            filename = "temp_{}_{}.tsv".format(project_id, node)
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
            outname = "temp_{}_{}.tsv".format(project_id, out_node)
            df.to_csv(outname, sep='\t', index=False, encoding='utf-8')
            print("\tTotal of {} records written to node {} in file {}.".format(len(df),out_node,outname))
            return df

    def add_missing_links(self,project_id,node,link):
        """
        This function adds missing links to a node's TSV when the parent node changes.
        Args:
            node (str): This is the node TSV to add links to.
            link (str): This is the name of the node to add links to.
        Example:
            This adds missing links to the visit node to the imaging_exam TSV.
            add_missing_links(node='imaging_exam',link='visit')
        """
        filename = "temp_{}_{}.tsv".format(project_id, node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo existing '{}' TSV found. Skipping...".format(node))
            return
        link_name = "{}s.submitter_id".format(link)
        df_no_link = df.loc[df[link_name].isnull()] # imaging_exams with no visit
        if len(df_no_link) > 0:
            df_no_link[link_name] = df_no_link['submitter_id'] + "_{}".format(link) # visit submitter_id is "<ESID>_visit"
            #df_no_link = df_no_link.drop(columns=['visits.id'])
            # Merge dummy visits back into original df
            df_link = df.loc[df[link_name].notnull()]
            df_final = pd.concat([df_link,df_no_link],ignore_index=True,sort=False)
            df_final.to_csv(filename, sep='\t', index=False, encoding='utf-8')
            print("\t{} links to '{}' added for '{}' in TSV file: {}".format(str(len(df_no_link)),link,node,filename))
            return df_final
        else:
            print("\tNo records are missing links to '{}' in the '{}' TSV.".format(link,node))
            return

    def create_missing_links(self,project_id,node,link,old_parent,properties):
        """
        This fxn creates links TSV for links in a node that don't exist.
        Args:
            node(str): This is the node TSV in which to look for links that don't exist.
            link(str): This is the node to create the link records in.
            old_parent(str): This is the parent node of 'node' prior to the dictionary change.
            properties(dict): Dict of required properties/values to add to new link records.
        Example:
            This will create visit records that don't exist in the visit TSV but are in the imaging_exam TSV.
            create_missing_links(node='imaging_exam',link='visit',old_parent='case',properties={'visit_label':'Imaging','visit_method':'In-person Visit'})
        """
        print("Creating missing '{}' records with links to '{}' for '{}'.".format(link,old_parent,node))
        filename = "temp_{}_{}.tsv".format(project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo existing {} TSV found. Skipping..".format(node))
            return
        link_name = "{}s.submitter_id".format(link)
        link_names = list(df[link_name])
        link_file = "temp_{}_{}.tsv".format(project_id,link)
        try:
            link_df = pd.read_csv(link_file,sep='\t',header=0,dtype=str)
            existing = list(link_df['submitter_id'])
            missing = set(link_names).difference(existing) #lists items in link_names missing from existing
            if len(missing) > 0:
                print("\tCreating {} records in '{}' with links to '{}' for missing '{}' links.".format(len(missing),link,old_parent,node))
            else:
                print("\tAll {} records in '{}' node have existing links to '{}'. No new records added.".format(len(df),node,link))
                return link_df.loc[link_df['submitter_id'].isin(link_names)]
        except FileNotFoundError as e:
            link_df = pd.DataFrame()
            print("\tNo '{}' TSV found. Creating new TSV for links.".format(link))
            missing = link_names
        parent_link = "{}.submitter_id".format(old_parent)
        new_links = df.loc[df[link_name].isin(missing)][[link_name,parent_link]]
        new_links = new_links.rename(columns={link_name:'submitter_id'})
        new_links['type'] = link
        for prop in properties:
            new_links[prop] = properties[prop]
        all_links = pd.concat([link_df,new_links],ignore_index=True,sort=False)
        all_links.to_csv(link_file,sep='\t',index=False,encoding='utf-8')
        print("\t{} new missing '{}' records saved into TSV file:\n\t{}".format(str(len(new_links)),link,link_file))
        return new_links

    def batch_add_visits(self,project_id,dd,links):
        """
        Adds 'Unknown' dummy visits to records in nodes that link to the 'case' node and have no link to the 'visit' node.
        Args:
            project_id(str): The project_id of the TSVs.
            dd(dict): The data dictionary. Get it with `dd=sub.get_dictionary_all()`.
            links(dict): A dict of nodes with links to remove, e.g., {'node':['link1','link2']}.
        Example:
            This adds 'visits.submitter_id' links to the 'allergy' node, and it then adds those new visits to the 'visit' TSV, lining the new visit records to the same 'case' records the 'allergy' records are linked to.
            batch_add_visits(project_id=project_id,links={'allergy': ['cases', 'treatments', 'medications']}
        """
        required_props={'visit_label':'Unknown','visit_method':'Unknown'}
        total = 0
        for node in list(links.keys()):
            # if the node has (only) a link to visit in new dd:
            targets = []
            node_links = dd[node]['links']
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
                    df = self.create_missing_links(project_id=project_id,node=node,link='visit',old_parent=links_to_drop[0],properties=required_props)
                    total += len(df)
            elif 'cases' in links_to_drop and 'visit' in targets and len(targets) == 1:
                df = self.add_missing_links(project_id=project_id,node=node,link='visit')
                if df is not None:
                    df = self.create_missing_links(project_id=project_id,node=node,link='visit',old_parent='cases',properties=required_props)
                    total += len(df)
            else:
                print("\tNo links to 'case' found in the '{}' TSV.".format(node))
        print("Total of {} missing visit links created for this batch.".format(total))

    def move_properties(self,project_id,from_node,to_node,properties,parent_node=None):
        """
        This function takes a node with properties to be moved (from_node) and moves those properties/data to a new node (to_node).
        Fxn also checks whether the data for properties to be moved actually has non-null data. If all data are null, no new records are created.
        Args:
            from_node(str): Node TSV to copy data from.
            to_node(str): Node TSV to add copied data to.
            properties(list): List of column headers containing data to copy.
            parent_node(str): The parent node that links the from_node to the to_node, e.g., 'visit' or 'case'.
        Example:
            This moves the property 'military_status' from 'demographic' node to 'military_history' node, which should link to the same parent node 'case'.
            move_properties(from_node='demographic',to_node='military_history',properties=['military_status'],parent_node='case')
        """
        print("Moving {} from '{}' to '{}'.".format(properties,from_node,to_node))
        from_name = "temp_{}_{}.tsv".format(project_id,from_node) #from imaging_exam
        try:
            df_from = pd.read_csv(from_name,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo '{}' TSV found with the data to be moved. Nothing to do. Finished.".format(from_node))
            return
        to_name = "temp_{}_{}.tsv".format(project_id,to_node) #to reproductive_health
        try:
            df_to = pd.read_csv(to_name,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            df_to = pd.DataFrame(columns=['submitter_id'])
            print("\tNo '{}' TSV found. Creating new TSV for data to be moved.".format(to_node))

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

        all_props = [parent_link] + properties
        new_to = df_from[all_props] #demo_case = demo[['cases.submitter_id']+static_case]

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
                #case_data = pd.DataFrame(columns=headers)
                for header in headers:
                    vals = list(df1.loc[df1[header].notnull()][header].unique())
                    if len(vals) == 1:
                        #case_data[header] = vals
                        case_data.loc[case_data['submitter_id']==case_id,header] = vals
                    elif len(vals) > 1:
                        print(vals)
                count += 1
            all_to = pd.merge(df_to,case_data,on='submitter_id', how='left')
        else:
            new_to['type'] = to_node
            new_to['project_id'] = project_id
            new_to['submitter_id'] = df_from['submitter_id'] + "_{}".format(to_node)
            #only write new_to records if submitter_ids don't already exist in df_to:
            add_to = new_to.loc[~new_to['submitter_id'].isin(list(df_to.submitter_id))]
            all_to = pd.concat([df_to,add_to],ignore_index=True,sort=False)
        all_to.to_csv(to_name,sep='\t',index=False,encoding='utf-8')
        print("\tProperties moved to '{}' node from '{}'. Data saved in file:\n\t{}".format(to_node,from_node,to_name))
        return all_to

    def change_property_names(self,project_id,node,properties):
        """
        Changes the names of columns in a TSV.
        Args:
            project_id(str): The project_id of the TSVs.
            node(str): The name of the node TSV to change column names in.
            properties(dict): A dict with keys of old prop names to change with values as new names.
        Example:
            This changes the column header "time_of_surgery" to "hour_of_surgery" in the surgery TSV.
            change_property_names(node='surgery',properties={'time_of_surgery','hour_of_surgery'})
        """
        filename = "temp_{}_{}.tsv".format(project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
            df.rename(columns=properties,inplace = True)
            df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
            print("Properties names changed and TSV written to file: \n\t{}".format(filename))
            return df
        except Exception as e:
            print("Couldn't change property names: {}".format(e))
            return

    def drop_properties(self,project_id,node,properties):
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
        filename = "temp_{}_{}.tsv".format(project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo '{}' TSV found. Skipping...".format(node))
            return
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

    def change_enum(self,project_id,node,prop,enums):
        """
        Changes an enumeration value in the data.
        Args:
            project_id(str): The project_id of the data.
            node(str): The node TSV to change enumeration values in.
            prop(str): The property (an enum) to change values for.
            enums(dict): A dict containing the mapping of {'old':'new'} enum values.
        Example:
            This changes all 'Percent' to 'Pct' in property 'test_units' of node 'lab_test'
            change_enum(project_id=project_id,node='lab_test',property='test_units',enums={'Percent':'Pct'})
        """
        print("{}:\n\tChanging values for property '{}'".format(node,prop))
        filename = "temp_{}_{}.tsv".format(project_id,node)
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
                print("\tEnum values changed in '{}' node and TSV written to file: \n\t{}".format(node,filename))
            else:
                print("\tNo enum values were changed in '{}' node. No TSVs changed.".format(node))
            return df
        except FileNotFoundError as e:
            print("\tNo TSV found for node '{}'.".format(node))

    def drop_links(self,project_id,node,links):
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
        filename = "temp_{}_{}.tsv".format(project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("\tNo '{}' TSV found. Skipping...".format(node))
            return
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

    def merge_links(self,project_id,node,link,links_to_merge):
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
        """
        # This fxn is mostly for merging the 7 imaging_exam subtypes into one imaging_exams link for imaging_file node. Not sure what other use cases there may be.
        # links_to_merge=['imaging_fmri_exams','imaging_mri_exams','imaging_spect_exams','imaging_ultrasonography_exams','imaging_xray_exams','imaging_ct_exams','imaging_pet_exams']
        filename = "temp_{}_{}.tsv".format(project_id,node)
        try:
            df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
        except FileNotFoundError as e:
            print("No '{}' TSV found. Skipping...".format(node))
            return
        link_name = "{}.submitter_id".format(link)
        df[link_name] = np.nan
        for sublink in links_to_merge:
            sid = "{}.submitter_id".format(sublink)
            df.loc[df[link_name].isnull(), link_name] = df[sid]
        df.to_csv(filename,sep='\t',index=False,encoding='utf-8')
        print("Links merged to '{}' and data written to TSV file: \n\t{}".format(link,filename))
        return df

    def get_submission_order(self,dd,project_id,prefix='temp',suffix='tsv'):
        pattern = "{}*{}".format(prefix,suffix)
        filenames = glob.glob(pattern)
        all_nodes = []
        suborder = {}
        for filename in filenames:
            #print("Found {}".format(filename))
            regex = "{}_{}_(.+).{}".format(prefix,project_id,suffix)
            #match = re.search('AAA(.+)ZZZ', text)
            match = re.search(regex, filename)
            if match:
                node = match.group(1)
                if node in list(dd):
                    all_nodes.append(node)
                else:
                    print("The node '{}' is not in the data dictionary! Skipping...".format(node))
        print("Found the following nodes:\n{}".format(all_nodes))
        checked = []
        while len(all_nodes) > 0:
            node = all_nodes.pop(0)
            print("Determining order for node {}".format(node))
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
                                print("Node {} has been checked before.".format(node))
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
        suborder = sorted(suborder.items(), key=operator.itemgetter(1))
        return suborder

    def drop_ids(self,project_id,suborder):
        """
        Drops the 'id' column from all the TSVs in 'suborder' dictionary obtained by running, e.g.:
        suborder = get_submission_order(dd,project_id,prefix='temp',suffix='tsv')
        """
        for node_order in suborder:
            node = node_order[0]
            filename = "temp_{}_{}.tsv".format(project_id,node)
            try:
                df = pd.read_csv(filename,sep='\t',header=0,dtype=str)
            except FileNotFoundError as e:
                print("\tNo existing {} TSV found. Skipping..".format(node))
                return
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

    def submit_file(self,project_id,filename,chunk_size=30,row_offset=0):
        """Submit data in a spreadsheet file containing multiple records in rows to a Gen3 Data Commons.
        Args:
            project_id (str): The project_id to submit to.
            filename (str): The file containing data to submit. The format can be TSV, CSV or XLSX (first worksheet only for now).
            chunk_size (integer): The number of rows of data to submit for each request to the API.
            row_offset (integer): The number of rows of data to skip; '0' starts submission from the first row and submits all data.
        Examples:
            This submits a spreadsheet file containing multiple records in rows to the CCLE project in the sandbox commons.
            >>> Gen3Submission.submit_file(api=api,project_id="DCF-CCLE",filename="data_spreadsheet.tsv")
        """
        f = os.path.basename(filename)
        if f.lower().endswith(".csv"):
            df = pd.read_csv(filename, header=0, sep=",", dtype=str).fillna("")
        elif f.lower().endswith(".xlsx"):
            xl = pd.ExcelFile(filename, dtype=str)  # load excel file
            sheet = xl.sheet_names[0]  # sheetname
            df = xl.parse(sheet)  # save sheet as dataframe
            converters = {col: str for col in list(df)}
            df = pd.read_excel(filename, converters=converters).fillna("")  # remove nan
        elif filename.lower().endswith((".tsv", ".txt")):
            df = pd.read_csv(filename, header=0, sep="\t", dtype=str).fillna("")
        else:
            raise Gen3UserError("Please upload a file in CSV, TSV, or XLSX format.")
        df.rename(columns={c: c.lstrip("*") for c in df.columns}, inplace=True)
        if len(list(df.submitter_id)) != len(list(df.submitter_id.unique())):
            raise Gen3Error("Warning: File contains duplicate submitter_ids.\n\t{}\n\tNote: submitter_ids must be unique within a node!".format(filename))
            return
        print("\nSubmitting {} with {} records.".format(filename, str(len(df))))
        program, project = project_id.split("-", 1)
        api_url = "{}/api/v0/submission/{}/{}".format(api,program,project)
        headers = {"content-type": "text/tab-separated-values"}
        start = row_offset
        end = row_offset + chunk_size
        chunk = df[start:end]
        count = 0
        results = {"invalid": {},"other": [],"details": [],"succeeded": [],"responses": []}
        while (start + len(chunk)) <= len(df):
            timeout = False
            valid_but_failed = []
            invalid = []
            count += 1
            print("Chunk {} (chunk size: {}, submitted: {} of {})".format(str(count),str(chunk_size),str(len(results["succeeded"]) + len(results["invalid"])),str(len(df)),))
            try:
                response = requests.put(self._endpoint,auth=self._auth_provider,data=chunk.to_csv(sep="\t", index=False),headers=headers,).text
            except requests.exceptions.ConnectionError as e:
                results["details"].append(e.message)
            if ("Request Timeout" in response or "413 Request Entity Too Large" in response or "Connection aborted." in response or "service failure - try again later" in response):  # time-out, response is not valid JSON at the moment
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
                    print("\t No code in the API response for Chunk {}: {}".format(str(count), json_res.get("message")))
                    print("\t {}".format(str(json_res.get("transactional_errors"))))
                    results["responses"].append("Error Chunk {}: {}".format(str(count), json_res.get("message")))
                    results["other"].append(json_res.get("message"))
                elif "code" not in json_res:
                    print("\t Unhandled API-response: {}".format(response))
                    results["responses"].append("Unhandled API response: {}".format(response))
                elif json_res["code"] == 200:  # success
                    entities = json_res.get("entities", [])
                    print("\t Succeeded: {} entities.".format(str(len(entities))))
                    results["responses"].append("Chunk {} Succeeded: {} entities.".format(str(count), str(len(entities))))
                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        results["succeeded"].append(sid)
                elif (json_res["code"] == 400 or json_res["code"] == 403 or json_res["code"] == 404):  # failure
                    entities = json_res.get("entities", [])
                    print("\tChunk Failed: {} entities.".format(str(len(entities))))
                    results["responses"].append("Chunk {} Failed: {} entities.".format(str(count), str(len(entities))))
                    for entity in entities:
                        sid = entity["unique_keys"][0]["submitter_id"]
                        if entity["valid"]:  # valid but failed
                            valid_but_failed.append(sid)
                        else:  # invalid and failed
                            message = str(entity["errors"])
                            results["invalid"][sid] = message
                            invalid.append(sid)
                    print("\tInvalid records in this chunk: {}".format(str(len(invalid))))
                elif json_res["code"] == 500:  # internal server error
                    print("\t Internal Server Error: {}".format(response))
                    results["responses"].append("Internal Server Error: {}".format(response))
            if (len(valid_but_failed) > 0 and len(invalid) > 0):  # if valid entities failed bc grouped with invalid, retry submission
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

    def create_project(self,program,project):
        # Create the program/project:
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

    def submit_tsvs(self,project_id,suborder):
        """
        Submits all the TSVs in 'suborder' dictionary obtained by running, e.g.:
        suborder = get_submission_order(dd,project_id,prefix='temp',suffix='tsv')
        """
        logname = "submission_logfile.txt"
        with open(logname, 'w') as logfile:
            for node_order in suborder:
                node = node_order[0]
                filename="temp_{}_{}.tsv".format(project_id,node)
                try:
                    data = self.sub.submit_file(project_id=project_id,filename=filename,chunk_size=1000)
                    print(data)
                    logfile.write(json.dumps(data)) #put in log file
                except Exception as e:
                    print(e)

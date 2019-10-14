import pandas as pd

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

profile = 'bc'
api = 'https://data.braincommons.org/' # BRAIN Commons
creds = '/Users/christopher/Downloads/bc-credentials.json'

auth = Gen3Auth(api, refresh_file=creds)
sub = Gen3Submission(api, auth) # Initialize an instance this class, using your creds in 'auth'

%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py # Download additional functions in Gen3Expansion class
exp = Gen3Expansion(api, auth) # Initialize an instance, using it like exp.get_project_tsvs()

dd = sub.get_dictionary_all()
schemas = list(dd)

excluded_schemas = ['_definitions','_settings','_terms','program','project','root','data_release','metaschema']
root_node = 'project'

nodes = [k for k in schemas if k not in excluded_schemas]

submission_order = [(root_node,0)]
while len(submission_order) < len(nodes)+1: # "root_node" is not in "nodes", thus the +1
    for node in nodes:

        if len([item for item in submission_order if node in item]) == 0: #if the node is not in submission_order
            print("Node: {}".format(node))
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

delete_order = sorted(submission_order, key=lambda x: x[1], reverse=True)





awk '!NF{$0=","}1' errors_file




errors_file='/Users/christopher/Documents/Notes/BHC/trouble-shooting/mjff-test_MJFF/errors.txt'
#errors_file='/Users/christopher/Documents/Notes/BHC/trouble-shooting/mjff-test_MJFF/errors_only.txt'
submission_file='/Users/christopher/Documents/Notes/BHC/trouble-shooting/mjff-test_MJFF/mjff-test_MJFF_sample_1471.tsv'


import pandas as pd

def get_entity_errors(errors_file,submission_file,write_tsvs=True):
    # Group entities in details into succeeded (successfully created/updated) and failed valid/invalid
    with open(errors_file, 'r') as file:
        f = file.read().rstrip('\n')
    chunks = f.split('\n\n')
    invalid = []
    messages = []
    valid = []
    succeeded = []
    responses = []
    results = {}
    chunk_count = 1
    for chunk in chunks:
        d = json.loads(chunk)
        if 'code' in d and d['code'] != 200:
            entities = d['entities']
            response = str('Chunk ' + str(chunk_count) + ' Failed: '+str(len(entities))+' entities.')
            responses.append(response)
            for entity in entities:
                sid = entity['unique_keys'][0]['submitter_id']
                if entity['valid']:
                    valid.append(sid)
                else:
                    messages.append(entity['errors'][0]['message'])
                    invalid.append(sid)
        elif 'code' not in d:
            responses.append('Chunk ' + str(chunk_count) + ' Timed-Out: '+str(d))
        else:
            entities = d['entities']
            response = str('Chunk ' + str(chunk_count) + ' Succeeded: '+str(len(entities))+' entities.')
            responses.append(response)
            for entity in entities:
                sid = entity['unique_keys'][0]['submitter_id']
                succeeded.append(sid)
        chunk_count += 1
    results['valid'] = valid
    results['invalid'] = invalid
    results['messages'] = messages
    results['succeeded'] = succeeded
    results['responses'] = responses
    submitted = succeeded + valid + invalid # 1231 in test data
    # Find the rows in submitted TSV that are not in either failed or succeeded, 8 time outs in test data, 8*30 = 240 records
    if write_tsvs is True:
        df = pd.read_csv(submission_file, sep='\t',header=0)
        missing_df = df.loc[~df['submitter_id'].isin(submitted)] # these are records that timed-out, 240 in test data
        valid_df = df.loc[df['submitter_id'].isin(valid)] # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
        invalid_df = df.loc[df['submitter_id'].isin(invalid)] # these are records that failed due to being invalid and should be reformatted

        sub_name = ntpath.basename(submission_file)
        missing_file = 'missing_' + sub_name
        valid_file = 'valid_' + sub_name
        invalid_file = 'invalid_' + sub_name
        missing_df.to_csv(missing_file, sep='\t', index=False, encoding='utf-8')
        valid_df.to_csv(valid_file, sep='\t', index=False, encoding='utf-8')
        invalid_df.to_csv(invalid_file, sep='\t', index=False, encoding='utf-8')
    return results

res = get_entity_errors(errors_file,submission_file,write_tsvs=True)



















schema = sub.get_dictionary_node('sample')
props = list(schema['properties'].keys())

paginate_query(node,project_id,props=['id','submitter_id'],chunk_size=1000)

def query_records(node,project_id,api,chunk_size=100):
    schema = sub.get_dictionary_node(node)
    props = list(schema['properties'].keys())
    links = list(schema['links'])
    # need to get links out of the list of properties because they're handled differently in the query
    link_names = []
    for link in links:
        link_list = list(link)
        if 'subgroup' in link_list:
            subgroup = link['subgroup']
            for sublink in subgroup:
                link_names.append(sublink['name'])
        else:
            link_names.append(link['name'])
    for link in link_names:
        if link in props:
            props.remove(link)
            props.append(str(link + '{id submitter_id}'))

    df = paginate_query(node,project_id,props,chunk_size)
    return df

df = query_records('sample','mjff-PPMI',api,chunk_size=500)








api = 'https://data.braincommons.org/'
graph_endpoint = api + 'api/v0/submission/graphql/'

af = open('/Users/christopher/Downloads/bc-credentials.json', 'r')
keys = json.load(af)
turl = api + 'user/credentials/cdis/access_token'
token = requests.post(turl, json=keys).json()
headers = {'Authorization': 'bearer '+ token['access_token']}
print("\nauthenticated\n") #debug

node = 'sample'
project_id = 'mjff-PPMI'
query_txt = """query Test { %s (first:0, project_id: "%s") {id}} """ % (node, project_id)
query = {'query': query_txt}
resp = requests.post(graph_endpoint, json=query, headers=headers).text # Get id from submitter_id
data = json.loads(resp)
data

api_url = "https://data.braincommons.org/api/v0/submission/graphql"

output = requests.post(api_url, auth=auth, json=query)

data = json.loads(output)

data = json.loads(json.dumps(output))

if "errors" in data:
    raise Gen3SubmissionQueryError(data["errors"])

if "error" in output:
    raise Gen3SubmissionQueryError(data["error"])
#'{"error": {"Request Timeout or Service Unavailable"}}'

if not "data" in data:
    print(query_txt)
    print(data)



    def get_dictionary_node(self, node_type):
        """Returns the dictionary schema for a specific node.

        This gets the current json dictionary schema for a specific node type in a commons.

        Args:
            node_type (str): The node_type (or name of the node) to retrieve.

        Examples:
            This returns the dictionary schema the "subject" node.

            >>> Gen3Submission.get_dictionary_node("subject")

        """
        api_url = "{}/api/v0/submission/_dictionary/{}".format(
            self._endpoint, node_type
        )
        output = requests.get(api_url).text
        data = json.loads(output)
        return data

    def get_dictionary_all(self):
        """Returns the entire dictionary object for a commons.

        This gets a json of the current dictionary schema for a commons.

        Examples:
            This returns the dictionary schema for a commons.

            >>> Gen3Submission.get_dictionary_all()

        """
        return self.get_dictionary_node("_all")

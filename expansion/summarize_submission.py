#!/usr/bin/env python3

# example:
# python summarize_submission.py --tsv submission.tsv --details details.txt --write_tsvs
# python summarize_submission.py --tsv submission.tsv --details details.txt

# If --write_tsvs is added as an option, it writes the following TSVs, which are subsets of the submission.tsv:
# missing_submission.tsv: This file contains records (that is, entities or rows in the TSV) that are missing from the details output, which typically means the chunk timed-out.
# valid_submission.tsv: This file contains valid records that failed because they were in a chunk with invalid entities. This TSV should be able to be submitted as-is without changes.
# invalid_submission.tsv: This file contains invalid records that failed because of a formatting error. These records each need reformatting before re-submitting.
# The error messages for invalid records are printed when the script is run.

# To Run interactively
# Copy and paste the summarize_submission function into ipython, e.g.:
# res = summarize_submission('submission.tsv','details.txt',write_tsvs=True)

import argparse, json, ntpath
import pandas as pd

parser = argparse.ArgumentParser(description="Summarize TSV submission details.")
parser.add_argument('-t', '--tsv', required=True, help="Filename of the submission TSV")
parser.add_argument('-d', '--details', required=True, help="Filename of the submission details.")
parser.add_argument('-w', '--write_tsvs', help="Do you want to write TSVs? 'True' or 'False'",  action='store_true')
args = parser.parse_args()


# Group entities in details into succeeded (successfully created/updated) and failed valid/invalid
def summarize_submission(tsv,details,write_tsvs):
    with open(details, 'r') as file:
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
                if entity['valid']: #valid but failed
                    valid.append(sid)
                else: #invalid and failed
                    message = entity['errors'][0]['message']
                    messages.append(message)
                    invalid.append(sid)
                    print('Invalid record: '+sid+'\n\tmessage: '+message)
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
    #get records missing in details from the submission.tsv
    df = pd.read_csv(tsv, sep='\t',header=0)
    missing_df = df.loc[~df['submitter_id'].isin(submitted)] # these are records that timed-out, 240 in test data
    missing = list(missing_df['submitter_id'])
    results['missing'] = missing

    # Find the rows in submitted TSV that are not in either failed or succeeded, 8 time outs in test data, 8*30 = 240 records
    if write_tsvs is True:
        print("Writing TSVs: ")
        valid_df = df.loc[df['submitter_id'].isin(valid)] # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
        invalid_df = df.loc[df['submitter_id'].isin(invalid)] # these are records that failed due to being invalid and should be reformatted
        sub_name = ntpath.basename(tsv)
        missing_file = 'missing_' + sub_name
        valid_file = 'valid_' + sub_name
        invalid_file = 'invalid_' + sub_name
        missing_df.to_csv(missing_file, sep='\t', index=False, encoding='utf-8')
        valid_df.to_csv(valid_file, sep='\t', index=False, encoding='utf-8')
        invalid_df.to_csv(invalid_file, sep='\t', index=False, encoding='utf-8')
        print('\t' + missing_file)
        print('\t' + valid_file)
        print('\t' + invalid_file)

    return results

summarize_submission(args.tsv,args.details,args.write_tsvs)

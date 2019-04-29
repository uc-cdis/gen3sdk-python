import requests
import sys
import getopt
import os
import argparse
import datetime
import time
import json
import pandas as pd

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

## Need to handle re-authentication for error 401:
## <Response [401]>{
# "message": "Authentication Error: Signature has expired"

class Gen3SubmitError(Exception):
	pass

class Gen3cli:
	"""Submit/Export/Query data from a Gen3 Submission system.

	A class for interacting with the Gen3 submission services.
	Supports submitting and exporting from Sheepdog.
	Supports GraphQL queries through Peregrine.

	Args:
		endpoint (str): The URL of the data commons.
		auth_provider (Gen3Auth): A Gen3Auth class instance.

	Examples:
		This generates the Gen3Submission class pointed at the sandbox commons while
		using the credentials.json downloaded from the commons profile page.

		>>> endpoint = "https://nci-crdc-demo.datacommons.io"
		... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
		... sub = Gen3Submission(endpoint, auth)

	"""

	def __init__(self, endpoint, auth_provider):
		self._auth_provider = auth_provider
		self._endpoint = endpoint

	def submit_record(self, program, project, json):
		"""Submit record(s) to a project as json.

		Args:
			program (str): The program to submit to.
			project (str): The project to submit to.
			json (object): The json defining the record(s) to submit. For multiple records, the json should be an array of records.

		Examples:
			This submits records to the CCLE project in the sandbox commons.

			>>> Gen3Submission.submit_record("DCF", "CCLE", json)

		"""
		api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
		output = requests.put(api_url, auth=self._auth_provider, json=json).text
		return output

	def submit_file(self, project_id, filename, chunk_size=30, row_offset=0):

		# Read the file in as a pandas DataFrame
		f = os.path.basename(filename)
		if f.lower().endswith('.csv'):
			df = pd.read_csv(filename, header=0, sep=',')
		elif f.lower().endswith('.xlsx'):
			xl = pd.ExcelFile(filename) #load excel file
			sheet = xl.sheet_names[0] #sheetname
			df = xl.parse(sheet) #save sheet as dataframe
		elif filename.lower().endswith(('.tsv','.txt')):
			df = pd.read_csv(filename, header=0, sep='\t')
		else:
			print("Please upload a file in CSV, TSV, or XLSX format.")
			exit()

		# Chunk the file
		print("\nSubmitting "+filename+" with "+str(len(df))+" records.")
		program,project = project_id.split('-',1)
		api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
		headers = {'content-type': 'text/tab-separated-values'}

		start = row_offset
		end = row_offset + chunk_size
		chunk = df[start:end]

		count = 0

		results = {'failed':{'messages':[],'submitter_ids':[]}, # these are invalid records
					'other':[], # any unhandled API responses
					'details':[], # entire API response details
					'succeeded':[], # list of submitter_ids that were successfully updated/created
					'responses':[], # list of API response codes
					'missing':[]} # list of submitter_ids missing from API response details

		while (start+len(chunk)) <= len(df):

			timeout = False
			valid = []
			invalid = []
			count+=1
			print("Chunk "+str(count)+" (chunk size: "+ str(chunk_size) + ", submitted: " + str(len(results['succeeded'])+len(results['failed']['submitter_ids'])) + " of " + str(len(df)) + "):  ")

			response = requests.put(api_url, auth=self._auth_provider, data=chunk.to_csv(sep='\t',index=False), headers=headers).text
			results['details'].append(response)

			if '"code": 200' in response:
				res = json.loads(response)
				entities = res['entities']
				print("\t Succeeded: "+str(len(entities))+" entities.")
				results['responses'].append("Chunk "+str(count)+" Succeeded: "+str(len(entities))+" entities.")
				#res = json.loads(response)
				for entity in entities:
					sid = entity['unique_keys'][0]['submitter_id']
					results['succeeded'].append(sid)

			elif '"code": 4' in response:
				res = json.loads(response)
				entities = res['entities']
				print("\tFailed: "+str(len(entities))+" entities.")
				results['responses'].append("Chunk "+str(count)+" Failed: "+str(len(entities))+" entities.")
				#res = json.loads(response)
				for entity in entities:
					sid = entity['unique_keys'][0]['submitter_id']
					if entity['valid']: #valid but failed
						valid.append(sid)
					else: #invalid and failed
						message = entity['errors'][0]['message']
						results['failed']['messages'].append(message)
						results['failed']['submitter_ids'].append(sid)
						invalid.append(sid)
				print("\tInvalid records in this chunk: " + str(len(invalid)))

			elif '"error": {"Request Timeout' in response:
				print("\t Request Timeout: " + response)
				results['responses'].append("Request Timeout: "+response)
				timeout = True

			elif '"code": 5' in response:
				print("\t Internal Server Error: " + response)
				results['responses'].append("Internal Server Error: " + response)

			elif '"message": ' in response and 'code' not in response:
				print("\t No code in the API response for Chunk " + str(count) + ": " + res['message'])
				print("\t " + str(res['transactional_errors']))
				results['responses'].append("Error Chunk " + str(count) + ": " + res['message'])
				results['other'].append(res['transactional_errors'])

			else:
				print("\t Unhandled API-response: "+response)
				results['responses'].append("Unhandled API response: "+response)

			if len(valid) > 0:
				chunk = chunk.loc[df['submitter_id'].isin(valid)] # these are records that weren't successful because they were part of a chunk that failed, but are valid and can be resubmitted without changes
				print("Retrying submission of valid entities from failed chunk: " + str(len(chunk)) + " valid entities.")

			elif timeout is False:
			#end of loop
				start+=chunk_size
				end = start + chunk_size
				chunk = df[start:end]

			else:
				chunk_size = int(chunk_size/2)
				end = start + chunk_size
				chunk = df[start:end]
				print("Retrying Chunk with reduced chunk_size: " + str(chunk_size))
				timeout = False

		print("Finished data submission.")
		print("Successful records: " + str(len(set(results['succeeded']))))
		print("Failed invalid records: " + str(len(set(results['failed']['submitter_ids']))))

		return results



# Testing:
api = 'https://nci-crdc-demo.datacommons.io/' # DCF  Sandbox Commons
profile = 'dcf'
creds = '/Users/christopher/Downloads/dcf-credentials.json'
auth = Gen3Auth(api, refresh_file=creds)
#sub = Gen3Submission(api, auth)
#file = Gen3File(api, auth)
cli = Gen3cli(api, auth)


#filename = 'subject_10000.tsv'
filename = 'invalid_subject_10000.tsv'
#filename = 'subject_100.tsv'
#filename = 'invalid_subject_100.tsv'
cli = Gen3cli(api, auth)

# submit_file(self, project_id, filename, chunk_size=30, row_offset=0):
#res = cli.submit_file(filename=filename,project_id='internal-training',chunk_size=50)
res = cli.submit_file(filename=filename,project_id='internal-training',chunk_size=5000)

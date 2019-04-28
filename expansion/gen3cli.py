import requests
import sys
import getopt
import os
import argparse
import datetime
import time
import json
import pandas as pd

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

	def read_file(self, filename):
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
			df = pd.DataFrame()
		return df


	def submit_file(self, project_id, filename, chunk_size=30, row_offset=0):
		program,project = project_id.split('-',1)

		df = Gen3cli.read_file(self, filename=filename)
		total_rows = len(df)
		print("Submitting "+filename+" with "+str(total_rows)+" records.")

		start = row_offset
		end = row_offset + chunk_size
		chunk = df[start:end]
		count = 0
		responses = []
		headers = {'content-type': 'text/tab-separated-values'}

		while (start+len(chunk)) <= total_rows:
			count+=1
			print("Chunk "+str(count)+": ")
			#itime = datetime.datetime.now()
			api_url = "{}/api/v0/submission/{}/{}".format(self._endpoint, program, project)
			#d = chunk.to_csv(sep='\t',index=False)
			#requests.put(api_url, auth=auth, data=d,headers=headers).text
			output = requests.put(api_url, auth=self._auth_provider, data=chunk.to_csv(sep='\t',index=False), headers=headers).text
			print(output)
			responses.append(output)
			#print(api_url)
			#print(chunk) #debug
			responses.append(str(chunk)) #debug

			if True:
			#end of loop
				start+=chunk_size
				end = start + chunk_size
				chunk = df[start:end]
			else:
				retry=1

		return responses

filename='subject_100.tsv'
#	def submit_file(self, project_id, filename, chunk_size=30, row_offset=0):
cli = Gen3cli(api, auth)
res = cli.submit_file(filename=filename,project_id='DCF-demo')


import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File
api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
profile = 'dcf'
creds = '/Users/christopher/Downloads/dcf-credentials.json'
auth = Gen3Auth(api, refresh_file=creds)
#sub = Gen3Submission(api, auth)
#file = Gen3File(api, auth)
cli = Gen3cli(api, auth)

s = cli.read_file('subject_100.tsv')
s.head()
d = cli.read_file('diagnosis_100.xlsx')
d.head()
d = cli.read_file('diagnosis_100.poop')
d.head()

df = cli.read_file('subject_100.tsv')

chunk1 = df[0:30]
chunk2 = df[30:60]
chunk3 = df[60:90]
chunk4 = df[90:120]

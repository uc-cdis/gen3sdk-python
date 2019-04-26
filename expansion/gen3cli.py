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

	def submit_file(project,filename,api,length,row,outdir='./submission_output',format='tsv'):
		"""Submit record(s) to a project as json.

		Args:
			program (str): The program to submit to.
			project (str): The project to submit to.
			json (object): The json defining the record(s) to submit. For multiple records, the json should be an array of records.

		Examples:
			This submits records to the CCLE project in the sandbox commons.

			>>> Gen3Submission.submit_record("DCF", "CCLE", json)

		"""
		token_url   = api + "/user/credentials/cdis/access_token"
		graphql_url = api + "/api/v0/submission/graphql/"
		project_url = api + "/api/v0/submission/" + project.replace('-', '/', 1) + '/'

		# # get keys
		# json_data = open(args.authfile).read()
		# keys = json.loads(json_data)
		# auth = requests.post(token_url, json=keys)

		header = ""
		data = ""
		count = 0
		total = -1
		nline = 0

		# create output directory if doesn't exist
		if not os.path.exists(outdir):
			os.makedirs(outdir)

		# if there are multiple dots, splitext splits at the last one (so splitext('file.jpg.zip') gives ('file.jpg', '.zip')
		arg_filename = os.path.basename(filename)
		outfile = outdir + "submission_output_" + os.path.splitext(filename)[0] + ".txt"
		i = 2
		while os.path.isfile(outfile):
			outfile = outdir + "submission_output_" + os.path.splitext(arg_filename)[0] + "_" + str(i) + ".txt"
			i += 1
		output = open(outfile, 'w')

		with open(filename, 'r') as file:
			for line in file:
				# if format is 'csv' or 'CSV':
				# 	line = line.replace(",", "\t")
				# if format is 'xlsx' or 'XLSX':
				# 	#read the file in with pandas
				if nline == 0:
					header = line
					data = header + "\r"
				nline += 1
				if nline > int(args.row):
					data = data + line + "\r"
					count = count + 1
					total = total + 1
					if count >= args.length:
						count = 1
						itime = datetime.datetime.now()

						req = requests.Request(method='PUT', url=project_url, headers={'content-type': 'text/tab-separated-values', 'Authorization': 'bearer '+ auth.json()['access_token']}, data=data)
						prepared = req.prepare()
						response = requests.Session().send(prepared)
						etime = datetime.datetime.now()
						print ("Submitted (" + str(total) + "): " + str(response) + " " + str(etime-itime))
						output.write("Submitted (" + str(total) + "): " + str(response))
						output.write(response.text)
						if "200" not in str(response):
							print ("Submission failed. Stopping...")
							break
						data = header + "\r"

		response = requests.put(project_url, data=data, headers={'content-type': 'text/tab-separated-values', 'Authorization': 'bearer '+ auth.json()['access_token']})
		print ("Submitted (" + str(total) + "): " + str(response))
		output.write("Submitted (" + str(total) + "): " + str(response))
		output.write(response.text)







	# from cgmeyer submission_v4
		print("\n in upload mode \n") #debug
		outfile = open_outfile()
		headers['content-type']='text/tab-separated-values'
		uurl = api + 'api/v0/submission/' + project.replace('-','/',1) + '/'
		print('\nopening '+args.file+' \n\n')
		with open(args.file, 'rU') as file:
			lines = file.readlines()
			print("\nFound " + str(len(lines)) + " lines in the tsv, including the header row.\n")
		file.close #is this necessary?
		header = data = ""
		count = 0
		total = -1
		del lines
		with open(args.file, 'rU') as file:
			for line in file:
				if is_csv:
					line = line.replace(",", "\t")
				if count == 0:
					header = line
				data = data + line + "\r"
				count += 1
				total += 1
				if count >= args.length:
					count = 1
					response = requests.put(uurl, data=data, headers=headers)
					if success.match(str(response)):
						print("Succeeded (" + str(total) + "): " + str(response))
					else:
						print("Failed (" + str(total) + "): " + str(response))
						failfile = open_failfile()
						failfile.write(data)
						failfile.close
					outfile.write("Submitted (" + str(total) + "): " + str(response))
					outfile.write(response.text)
					data = header + "\r"
		response = requests.put(uurl, data=data, headers=headers)
		if fail.match(str(response)):
			print("\nFailed\n")
			failfile = open_failfile()
			failfile.write(data)
			failfile.close
		else:
			print("\nSuccess\n")
		print("Submitted (" + str(total) + "): " + str(response))
		outfile.write("Submitted (" + str(total) + "): " + str(response))
		outfile.write(response.text)
		outfile.close
		print("\n finished upload script \n") #debug

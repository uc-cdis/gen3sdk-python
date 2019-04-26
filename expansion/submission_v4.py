#!/usr/bin/env python2

import requests, json
import sys
import getopt
import os
import argparse

import re
fail = re.compile("^<Response \[400\]>$")
success = re.compile("^<Response \[200\]>$")

# download example:
# python2.7 submission_v4.py -m download -a bc -p mjff-test_MJFF -k /Users/christopher/Downloads/credentials.json -n case

# upload example:
# python2.7 submission_v4.py -m upload -a bc -p mjff-test_MJFF -k /Users/christopher/Downloads/credentials.json -l 100 -f /Users/christopher/Documents/Notes/BHC/template_tsvs/training_tsvs/valid/test_case.tsv
# python2.7 submission_v4.py -m upload -a va -p VA-REPOP-CONSENTED -k /Users/christopher/Downloads/va-credentials.json -l 100 -f /Users/christopher/Documents/Notes/VA/data_collection/tsvs/data_collection/VA_image_exam_file.txt
# python2.7 submission_v4.py -m upload -a genomel -p genomel-pmu -k /Users/christopher/Downloads/genomel-credentials.json -l 50 -f /Users/christopher/Documents/Notes/genomel/genomel-pmu/submitted_unaligned_reads.tsv

# delete example:
# python2.7 submission_v4.py -m delete -a bc -p mjff-test_MJFF -k /Users/christopher/Downloads/credentials.json -l 100 -n case

# delete_project example:
# python2.7 submission_v4.py -m delete_project -a bc -k /Users/christopher/Downloads/credentials.json -p mjff-FSToo

# query example:
# python2.7 submission_v4.py -m query -a bc -p mjff-test_MJFF -k /Users/christopher/Downloads/credentials.json -n case


parser = argparse.ArgumentParser(description="working with data commons api")
parser.add_argument('-m', '--mode', required=True, help="specify mode: upload, download, delete, or query")
parser.add_argument('-a', '--api', required=True, help="specify data commons api, e.g.: bpa, bc or genomel")
parser.add_argument('-p', '--project', required=True, help="specify the project name, e.g.,: bpa-MyOrg_P0001_T1")
parser.add_argument('-k', '--authfile', required=True, help="location of credentials.json")
# for upload:
parser.add_argument('-f', '--file', required=False, default='nofile.tsv', help="meta data file to submit in tsv format")
parser.add_argument('-l', '--length', type=int, default=100, required=False, help="length of the chunk to be submitted")
parser.add_argument('-o', '--offset', type=int, default=0, required=False, help="specify the offset for graphQL queries. Or line offset if submitting large TSV; offset = 1000 means start at line 1000 of TSV.")
# for query:
parser.add_argument('-n', '--node', required=False, default='none', help="metadata node to query or delete")
args = parser.parse_args()


# API endpoints:

api = args.api
if api == "account":
	api = "https://acct.bionimbus.org/"
elif api == "bpa":
	api = "https://data.bloodpac.org/"
elif api == "bc":
	api = "https://data.braincommons.org/"
elif api == "genomel":
	api = "https://genomel.bionimbus.org/"
elif api == "gtex":
	api = "https://gtex.bionimbus.org/"
elif api == "niaid":
	api = "https://niaid.bionimbus.org/"
elif api == "va":
	api = "https://vpodc.org/"
elif api == "cvb":
	api = "https://cvbcommons.org/"
print("\nset api to "+api+'\n') #debug

# Authentication:
af = open(args.authfile, 'r')
keys = json.load(af)
turl = api + 'user/credentials/cdis/access_token'
token = requests.post(turl, json=keys).json()
headers = {'Authorization': 'bearer '+ token['access_token']}
print("\nauthenticated\n") #debug

# Open output file:
def open_outfile():
	if args.file=='nofile.tsv':
		if args.node!='none':
			tsv = args.node + '.tsv'
		else:
			tsv = args.file
	else:
		tsv = args.file
	filename = os.path.split(tsv)[1]
	if not os.path.exists('./output/'):
		os.makedirs('./output/')
	outname = './output/'+args.api+'_'+args.mode+'_'+args.project+'_'+os.path.splitext(filename)[0]+'_1.txt'
	i = 2
	while os.path.isfile(outname):
	    outname = './output/'+args.api+'_'+args.mode+'_'+args.project+'_'+os.path.splitext(filename)[0]+'_'+str(i)+'.txt'
	    i += 1
	outfile = open(outname, 'w')
	print('\nopened output file '+outname+'\n') #debug
	return(outfile)

def open_failfile():
	if args.file=='nofile.tsv':
		if args.node!='none':
			tsv = args.node + '.tsv'
		else:
			tsv = args.file
	else:
		tsv = args.file
	filename = os.path.split(tsv)[1]
	if not os.path.exists('./output/failed/'):
		os.makedirs('./output/failed/')
	outname = './output/failed/'+args.api+'_'+args.mode+'_'+args.project+'_'+os.path.splitext(filename)[0]+'_failed_1.txt'
	i = 2
	while os.path.isfile(outname):
	    outname = './output/failed/'+args.api+'_'+args.mode+'_'+args.project+'_'+os.path.splitext(filename)[0]+'_failed_'+str(i)+'.txt'
	    i += 1
	failfile = open(outname, 'w')
	print('\nopened failed data submission file '+outname+'\n') #debug
	return(failfile)

def query_api(node = args.node, project_id = args.project):
	query_txt = """query Test { %s (first:0, project_id: "%s") {id}} """ % (node, project_id)
	query = {'query': query_txt}
	gurl = api + 'api/v0/submission/graphql/'
	resp = requests.post(gurl, json=query, headers=headers).text # Get id from submitter_id
	data = json.loads(resp)
	return(data)

##########################
############# UPLOAD
##########################
# new method:
# ul = requests.put(uurl, data=data, headers={'content-type': 'text/tab-separated-values', 'Authorization': 'bearer '+ t.json()['access_token']})
# print(ul.text)

if args.mode == "upload":
	print("\n in upload mode \n") #debug
	outfile = open_outfile()
	project = args.project
	# Add re-authentication
	headers['content-type']='text/tab-separated-values'
	uurl = api + 'api/v0/submission/' + project.replace('-','/',1) + '/'
	print('\nopening '+args.file+' \n\n')
	with open(args.file, 'rU') as file:
		lines = file.readlines()
		print("\nFound " + str(len(lines)) + " lines in the tsv, including the header row.\n")
	file.close
	header = ""
	data = ""
	count = 0
	total = -1
	del lines
	with open(args.file, 'rU') as file:
		for line in file:
			if count == 0:
				header = line
			data = data + line + "\r"
			count += 1
			total += 1
			if count >= args.length:
				count = 1
				# Re-authentication, necessary to avoid 401 error for large files
				token = requests.post(turl, json=keys).json()
				headers['Authorization']='bearer '+ token['access_token']
				print("\nauthenticated\n") #debug
				response = requests.put(uurl, data=data, headers=headers)
				if success.match(str(response)):
					print("Succeeded (" + str(total) + "): " + str(response))
				else:
					print("Failed (" + str(total) + "): " + str(response))
					failfile = open_failfile()
					failfile.write(data)
					failfile.close
#				outfile.write("Submitted (" + str(total) + "): " + str(response))
				outfile.write('\r'+response.text)
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


# datafile DOWNLOAD
if args.mode == 'datafile':
	print("\n in datafile download mode \n")
	project = args.project
	with open(args.file, 'rU') as file:
		lines = file.readlines()
		print("\nFound " + str(len(lines)) + " lines (uuids) in the manifest file.\n")
	file.close
	file_id = 'd7a509ec-4a4a-4ece-8a4c-678753583014'
	furl = api + 'user/data/download/' + file_id
	print(furl)
	resp = requests.get(furl, headers=headers).text # Get id from submitter_id
	print(resp)
	outfile = open_outfile()
	outfile.write(requests.get(json.loads(resp)['url']).text)
	outfile.close
	for file_id in lines:
		outfile = open_outfile()
		furl = api + 'user/data/download/' + file_id
		print(furl)
		resp = requests.get(furl, headers=headers).text # Get id from submitter_id
		print(resp)
		outfile.write(requests.get(json.loads(resp)['url']).text)
		outfile.close

# https://data.braincommons.org/user/data/download/d7a509ec-4a4a-4ece-8a4c-678753583014
# https://data.braincommons.org/api/v0/submission/mjff/test-MJFF/export?format=json&ids=<uuid>

##########################
############# DOWNLOAD
##########################
#new method:
#	durl = api + 'api/v0/submission/' + args.project.replace('-','/',1) + '/' + 'export?format=tsv&ids=' + ids[0:-1]
#	dl = requests.get(durl, headers={'Authorization': 'bearer '+ t.json()['access_token']})
#	print(dl.text)

elif args.mode == "download":
	outfile = open_outfile()
	node = args.node
	project_id = args.project
	print("\n starting download script \n") # Download URL = 'https://data.braincommons.org/api/v0/submission/mjff/test-MJFF/export?format=json&ids=<uuid>'
	query_txt = """query Test { %s (first:%s, offset:%s, project_id: "%s") {id}} """ % (node, args.length, args.offset, project_id)
	query = {'query': query_txt}
	print(query) #debug
	outfile.write(str(query)+"\n")
	gurl = api + 'api/v0/submission/graphql/'
	resp = requests.post(gurl, json=query, headers=headers).text # Get id from submitter_id
	print(resp) #debug
	#print output
	data = json.loads(resp)
	#print data
	print("\n\nTotal of "+str(len(data["data"][node]))+" entities in "+node+" node.")
	list_ids = ""
	count = 0
	total = 0
	for id in data['data'][node]:
		list_ids += id['id'] + ","
		count +=1
		if count >= args.length:
			durl = api + 'api/v0/submission/' + project_id.replace('-','/',1) + '/export?format=tsv&ids=' + list_ids[0:-1]
			resp = requests.get(durl, headers=headers)
			t = resp.text
			t = t.split("\r\n")
			t = [i for i in t if len(i) != 0]
			hd = t[0]
			if total == 0:
				outfile.write(hd)
				total += 1
			for i in range(1,len(t)):
				outfile.write("\n"+t[i])
				total += 1
			count = 0
			list_ids = ""
	durl = api + 'api/v0/submission/' + project_id.replace('-','/',1) + '/export?format=tsv&ids=' + list_ids[0:-1]
	resp = requests.get(durl, headers=headers)
	t = resp.text
	t = t.split("\r\n")
	t = [i for i in t if len(i) != 0]
	hd = t[0]
	if total == 0:
		outfile.write(hd)
	for i in range(1,len(t)):
		outfile.write("\n"+t[i])
		total += 1
	print("\nTotal of "+str(total)+" lines written to file " + outfile.name +", including header.\n\n")
	outfile.close
	print("\n finished download workflow \n") #debug

##########################
############# DELETE
##########################
### NEW:
#	rurl = 'https://data.bloodpac.org/api/v0/submission/internal/test/entities/a1ecde92-66ae-4d11-8009-475c2f34faa0,df2d8bc5-4aba-4951-afed-e079259c2cfd,3e640fbb-22f5-4278-9eeb-4e600b64f1a2'
#	dr = requests.delete(rurl, headers=headers).text
#	print(dr)

elif args.mode == "delete":
	outfile = open_outfile()
	print("\n starting delete workflow \n") #debug
	node = args.node
	project_id = args.project
	query_txt = """query Test { %s (first:0, project_id: "%s") {id}} """ % (node, project_id)
	query = {'query': query_txt}
	gurl = api + 'api/v0/submission/graphql/'
	resp = requests.post(gurl, json=query, headers=headers).text # Get id from submitter_id
	data = json.loads(resp)
	print("\n\nTotal of "+str(len(data["data"][node]))+" entities in "+node+" node.")
	list_ids = ""
	count = 0
	total = 0
	for id in data['data'][node]:
		list_ids += id['id'] + ","
		count +=1
		total +=1
		if count >= args.length:
			rurl = api + 'api/v0/submission/' + project_id.replace('-','/',1) + '/entities/' + list_ids[0:-1]
			resp = requests.delete(rurl, headers=headers)
			outfile.write(resp.text)
			print('\nDeleted ('+str(total)+'): '+str(resp)+' records from node ' + node +'. \n\n')
			count = 0
			list_ids = ""
	rurl = api + 'api/v0/submission/' + project_id.replace('-','/',1) + '/entities/' + list_ids[0:-1]
	resp = requests.delete(rurl, headers=headers)
	outfile.write(resp.text)
	print('\nDeleted ('+str(total)+'): '+str(resp)+' records from node ' + node +'. \n\n')
	print('\n finished delete workflow \n') #debug
	outfile.close


###############################
############# DELETE PROJECT
###############################
#You should be able to delete projects via requests.delete(URL/program/project)

elif args.mode == "delete_project":
	outfile = open_outfile()
	print("\n starting delete project workflow \n") #debug
	node = args.node
	project_id = args.project
	rurl = api + 'api/v0/submission/' + project_id.replace('-','/',1)
#	rurl = api + project_id.replace('-','/',1)
	print('\nSending request url: '+rurl+'. \n\n')
	resp = requests.delete(rurl, headers=headers)
	outfile.write(resp.text)
	print('\nDelete project '+project_id+' response: '+str(resp)+resp.text+'. \n\n')
	print('\n finished delete project workflow \n') #debug
	outfile.close

##########################
############# QUERY
##########################


elif args.mode == "query":
	outfile = open_outfile()
	print("\n starting query workflow \n") #debug
	data = query_api(node=args.node,project_id=args.project)
	print(data)
	outfile.write(json.dumps(data))
	outfile.close
	#data = json.loads(output.text)

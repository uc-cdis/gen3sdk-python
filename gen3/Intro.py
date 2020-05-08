####################################################################################
####################################################################################
####################################################################################
### Copy and paste code to run locally in "ipython" running in Mac terminal
​
## Set api endpoint of the data commons, and path to your credentials.json file downloaded from {api}/identity page
# goto https://data.bloodpac.org and login
# then goto https://data.bloodpac.org/identity and click "Create API key" and save to your Downloads folder below

#BB: hold off on BloodPAC
#api = 'https://data.bloodpac.org/' # BloodPAC
#cred = '/Users/bowenbao/Downloads/bpa-credentials.json'
​
api = 'https://gen3.datacommons.io/' # open access demo commons (OADC)
cred = '/Users/bowenbao/Documents/Project/credentials.json'
​
# Import the Gen3 Python SDK from my local copy of it downloaded from GitHub
​
import pandas as pd
import sys
​
git_dir='/Users/bowenbao/GitHub'
sdk_dir='/uc-cdis/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from gen3.auth import Gen3Auth # class for authentication
from gen3.submission import Gen3Submission # class for data upload/download/query
#analysis still has issues
from gen3.analysis import Gen3Analysis # class for new analysis functions
​
auth = Gen3Auth(api, refresh_file=cred)
sub = Gen3Submission(api, auth) # Initialize an instance this class, using your creds in 'auth'
​analysis = Gen3Analysis(api, auth) # Initialize an instance this class, using your creds in 'auth'

!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
%run expansion.py # Some additional functions in Gen3Expansion class
exp = Gen3Expansion(api, auth) # Initialize an instance, using its functions like exp.get_project_tsvs()
​
# Alternatively, to import functions you could also download the sdk files directly from GitHub and run them in ipython (e.g., if you didn't want to clone the whole GitHub repo)
#!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
#%run ./submission.py
#!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/auth.py
#%run ./auth.py
#!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/analysis.py
#%run ./analysis.py
​
# Run commands like this:
query_txt = """ { subject (first:0) {project_id submitter_id id index_date} }"""
query_data = sub.query(query_txt) #runs the Gen3Submission.query() function
​
projects = exp.get_project_ids()
​
#trial data
exp.get_project_tsvs(projects='GEO-GSE63878',outdir='project_tsvs_02282020')
​

####################################################################
api = 'https://gen3.datacommons.io/' 
cred = '/Users/bowenbao/Documents/Project/credentials.json'
import pandas as pd
import sys
git_dir='/Users/bowenbao/GitHub'
sdk_dir='/uc-cdis/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from gen3.auth import Gen3Auth 
from gen3.submission import Gen3Submission 
from gen3.analysis import Gen3Analysis 
auth = Gen3Auth(api, refresh_file=cred)
sub = Gen3Submission(api, auth) 

​analysis = Gen3Analysis(api, auth) 

!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
%run expansion.py 
exp = Gen3Expansion(api, auth) 
query_txt = """ { subject (first:0) {project_id submitter_id id index_date} }"""
query_data = sub.query(query_txt) 
projects = exp.get_project_ids()

####################################################################

#use this

####################################################################
api = 'https://gen3.datacommons.io/' 
cred = '/Users/bowenbao/Documents/Project/credentials.json'
import pandas as pd
import sys
project_dir = "/Users/bowenbao/Documents/Project/"
sys.path.insert(1, '{}'.format(project_dir))
from gen3.auth import Gen3Auth 
from gen3.submission import Gen3Submission 
#from gen3.analysis import Gen3Analysis 
auth = Gen3Auth(api, refresh_file=cred)
sub = Gen3Submission(api, auth) 
%run /Users/bowenbao/Documents/Project/gen3/analysis.py

​analysis = Gen3Analysis(api, auth) 

!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
%run expansion.py 
exp = Gen3Expansion(api, auth) 
query_txt = """ { subject (first:0) {project_id submitter_id id index_date} }"""
query_data = sub.query(query_txt) 
projects = exp.get_project_ids()

#not in ipython 
#change manually, ipython default still in bowenbao
os.chdir("Documents/Project")
#df = pd.read_csv('lab_test.tsv',sep='\t') 
df = pd.read_csv('lab_test2.tsv',sep='\t') 

#re-run 
%run /Users/bowenbao/Documents/Project/gen3/analysis.py
​analysis = Gen3Analysis(api, auth) 

dir(analysis) 

df.dtypes

df.dtypes["activity_area"]

###Testing 

analysis.plot_categorical_property(property = "sample_composition",df = df) 	#works

analysis.plot_categorical_property_by_order(property = "sample_composition",df = df) #works

analysis.plot_numeric_property(df =df,property = "activity_area") #works

#needs work - how to group by project
analysis.plot_numeric_property(df =df,property = "activity_area", by_project=True) #only one project #needs work

#needs work
analysis.node_record_counts(project_id = "OpenAccess-CCLE")	#query_txt = """{node (first:-1, project_id:"%s"){type}}""" problem 

analysis.property_counts_table("sample_composition", df) #works 

analysis.property_counts_by_project("sample_composition", df) #works

#table is not defined - has issue importing 
analysis.save_table_image(df,filename='mytable.png') 

#new
analysis.plotviolin_numeric_property_by_categorical_property(df =df, numeric_property = "activity_area", categorical_property = "sample_composition")

analysis.pie_categorical_property_count(property = "sample_composition", df = df)

analysis.scatter_numeric_by_numeric(df, "activity_area", "max_activity")

analysis.scatter_lognumeric_by_lognumeric(df, "activity_area", "max_activity") #need work

#terminal push
alias gb='git branch'
alias gc='git commit -a -m'
alias gd='git pull origin develop'
alias gen3='cd ~/.gen3'
alias gf='git fetch'
alias gh='history | grep'
alias github='cd /Users/christopher/Documents/GitHub/'
alias gl='git pull'
alias gm='git pull origin master'
alias gp='git push origin "$(git branch | grep ^* | cut -c 3-)"'

#1 copy your updated analysis.py file to your local copy of my SDK github repo:
cp /Users/bowenbao/Documents/Project/gen3/analysis.py ~/github/cgmeyer/gen3sdk-python/gen3/analysis.py
#2 to add the new file
git add -A
#3 commit changes
git commit -a -m "I made some updates"
#4 push local changes in your branch ("bowen") to github.com
git push origin bowen 
#5 Open a pull request on github.com if there isn't already one open (there should already be one right now: https://github.com/cgmeyer/gen3sdk-python/pull/22)
#Click on pull request, open "Files Changed"

############################################################################################
############################################################################################
# This version is for the Gen3 Workspace JupyterHub:
# Add these commands to cells in a Python 3 Jupyter notebook.
​
# Get the gen3sdk and expansion functions
import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re
import pandas as pd
from pandas.io.json import json_normalize
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
​
# make a directory for the gen3 sdk/client in the 'pd' (persistent drive)
home_dir = "/home/jovyan/pd" # this is the *persistent* directory of the Gen3 workspace. Anything outside "pd" will not be saved (it's in VM's ephemeral memory).
gen3_dir = "{}/gen3".format(home_dir)
!mkdir -p ${gen3_dir}
os.chdir(gen3_dir)
​
# Get the latest gen3 SDK files
!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/auth.py
!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/analysis.py
!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
​
# Run the SDK files
%run ./auth.py
%run ./submission.py
%run ./analysis.py
%run ./expansion.py
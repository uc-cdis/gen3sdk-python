####################################################################################
####################################################################################
####################################################################################
### Run Locally in "ipython":

## Set gen3-client profile name, api endpoint of the data commons, and path to credentials file
profile = 'bc'
api = 'https://data.braincommons.org/' # BRAIN Commons
creds = '/Users/christopher/Downloads/bc-credentials.json'

profile = 'qa-brain'
api = 'https://qa-brain.planx-pla.net/'
creds = '/Users/christopher/Downloads/qa-credentials.json'

profile = 'gen3qa-brain'
api = 'https://gen3qa.braincommons.org/' # Gen3QA for BRAIN Commons
creds = '/Users/christopher/Downloads/gen3qa-brain-credentials.json'

profile = 'staging'
api = 'https://data-staging.braincommons.org/' # BRAIN Commons
creds = '/Users/christopher/Downloads/brain-staging-credentials.json'

profile = 'bpa'
api = 'https://data.bloodpac.org/' # BloodPAC
creds = '/Users/christopher/Downloads/bpa-credentials.json'

profile = 'ndh'
api = 'https://niaid.bionimbus.org/'
creds = '/Users/christopher/Downloads/ndh-credentials.json'

profile = 'vpodc'
api = 'https://vpodc.org/' #
creds = '/Users/christopher/Downloads/vpodc-credentials.json'

profile = 'dcf'
api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
creds = '/Users/christopher/Downloads/dcf-credentials.json'

profile = 'stage'
api = 'https://gen3.datastage.io/' # STAGE (old "DCP")
creds = '/Users/christopher/Downloads/stage-credentials.json'

# api = 'https://dcf-interop.kidsfirstdrc.org/' #Kids First


# Import the Gen3 Python SDK from my local copy of it downloaded from GitHub
# use this command to get the sdk: `git clone git@github.com:cgmeyer/gen3sdk-python.git`
import pandas as pd
import sys

sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/')
from expansion.expansion import Gen3Expansion
from migration import Gen3Migration
auth = Gen3Auth(api, refresh_file=creds)
sub = Gen3Submission(api, auth) # Initialize an instance this class, using your creds in 'auth'
%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py # Some additional functions in Gen3Expansion class
exp = Gen3Expansion(api, auth) # Initialize an instance, using it like exp.get_project_tsvs()

#!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py # get the latest beta version in GitHub
#%run ./submission.py # run the latest version in GitHub

# Download and configure gen3-client in Jupyter Notebook
!curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*osx |  cut -d '"' -f 4 | wget -qi -
!unzip dataclient_osx.zip
!mv gen3-client /Users/christopher/.gen3
!rm dataclient_osx.zip

# Now configure your profile:
client = 'gen3-client'
cmd = "{} configure --profile={} --apiendpoint={} --cred={}".format(client, profile, api, creds)
auth_cmd = "{} auth --profile={}".format(client, profile)
try:
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
    output = subprocess.check_output(auth_cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
    print(output)
except Exception as e:
    output = e.output.decode('UTF-8')
    print("ERROR:" + output)
























############################################################################################
############################################################################################
# This version is for the Gen3 Workspace JupyterHub:
# Run this in a Python 3 Jupyter notebook.

# Get the gen3sdk and expansion functions
import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re
import pandas as pd
from pandas.io.json import json_normalize
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

home_dir = "/home/jovyan/pd/"
gen3_dir = "{}gen3".format(home_dir)
!mkdir -p ${gen3_dir}
os.chdir(gen3_dir)

# Get the latest gen3 SDK files
!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/auth.py
!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py

# Run the SDK files
%run ./auth.py
%run ./submission.py
%run ./expansion.py

# Set gen3-client profile name, data commons API URL, and credentials location.
profile = 'dcf'
api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
creds = '/Users/christopher/Downloads/dcf-credentials.json'

# Create instances of the gen3sdk classes for a data commons API and matching credentials file.
auth = Gen3Auth(api, refresh_file=creds)
exp = Gen3Expansion(api, auth)
sub = Gen3Submission(api, auth)

# Get the latest gen3-client
!curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget -qi -
!unzip dataclient_linux.zip
!rm dataclient_linux.zip
client = "{}/gen3-client".format(gen3_dir)

#!/home/jovyan/gen3/gen3-client configure --profile=bpa --apiendpoint=https://data.bloodpac.org --cred=/home/jovyan/pd/bpa-credentials.json
# Configure a profile
config_cmd = client +' configure --profile='+profile+' --apiendpoint='+api+' --cred='+creds
try:
    output = subprocess.check_output(config_cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
except Exception as e:
    output = e.output.decode('UTF-8')
    print("ERROR:" + output)

# Check authorization privileges
auth_cmd = client +' auth --profile='+profile
try:
    output = subprocess.check_output(auth_cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
    print("\n"+output)
except Exception as e:
    output = e.output.decode('UTF-8')
    print("ERROR:" + output)

## Install gen3sdk via pip
# !pip install --force --upgrade gen3 --ignore-installed certifi
# import gen3
# from gen3.auth import Gen3Auth
# from gen3.submission import Gen3Submission
# from gen3.file import Gen3File
#

### RUN IN JUPYTER
# # Install gen3sdk via pip
!pip install --force --upgrade gen3 --ignore-installed certifi
#
# Import some Python packages
import requests, json, fnmatch, os, os.path, sys, subprocess, glob, ntpath, copy, re
import pandas as pd
from pandas.io.json import json_normalize
from collections import Counter

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Create the gen3sdk objects for authentication and submission
api = 'https://datacommons.org'
creds = '/home/jovyan/pd/my-credentials.json'

auth = Gen3Auth(api, refresh_file=creds)
# sub = Gen3Submission(api, auth)
# file = Gen3File(api, auth)
#
# !wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
# %run ./submission.py
# sub = Gen3Submission(api, auth)
#
#
# # Get the gen3sdk expansion functions
# !rm -f -- expansion.py
# !wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
# %run ./expansion.py
# exp = Gen3Expansion(api, auth)
#

# Download and configure gen3-client in Jupyter Notebook
profile = 'prof'
client = '/home/jovyan/gen3/gen3-client'

!curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget -qi -
!unzip dataclient_linux.zip
!mkdir -p /home/jovyan/pd/gen3
!mv gen3-client /home/jovyan/pd/gen3
!rm dataclient_linux.zip

curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget -qi -
unzip dataclient_linux.zip
mkdir -p /home/jovyan/pd/gen3
mv gen3-client /home/jovyan/pd/gen3
rm dataclient_linux.zip

#!/home/jovyan/.gen3/gen3-client configure --profile=bpa --apiendpoint=https://data.bloodpac.org --cred=/home/jovyan/pd/bpa-credentials.json
# Configure a profile
cmd = client +' configure --profile='+profile+' --apiendpoint='+api+' --cred='+creds
try:
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
except Exception as e:
    output = e.output.decode('UTF-8')
    print("ERROR:" + output)

# Check authorization privileges
cmd = client +' auth --profile='+profile
try:
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
    print("\n"+output)
except Exception as e:
    output = e.output.decode('UTF-8')
    print("ERROR:" + output)


####################################################################################
####################################################################################
####################################################################################
### Run Locally in "ipython":
## Import some Python packages

!pip install --force --upgrade gen3 --ignore-installed certifi

import pandas as pd

import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.file import Gen3File

## Set gen3-client profile name, api endpoint of the data commons, and path to credentials file
api = 'https://gen3qa.braincommons.org/' # Gen3QA for BRAIN Commons
creds = '/Users/christopher/Downloads/gen3qa-brain-credentials.json'


profile = 'bc'
api = 'https://data.braincommons.org/' # BRAIN Commons
creds = '/Users/christopher/Downloads/bc-credentials.json'

profile = 'staging'
api = 'https://data-staging.braincommons.org/' # BRAIN Commons
creds = '/Users/christopher/Downloads/brain-staging-credentials.json'


profile = 'bpa'
api = 'https://data.bloodpac.org/' # BloodPAC
creds = '/Users/christopher/Downloads/bpa-credentials.json'

profile = 'ndh'
api = 'https://niaid.bionimbus.org/'
creds = '/Users/christopher/Downloads/ndh-credentials.json'

api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
profile = 'dcf'
creds = '/Users/christopher/Downloads/dcf-credentials.json'

# api = 'https://dcf-interop.kidsfirstdrc.org/' #Kids First
#
# profile = 'stage'
# api = 'https://gen3.datastage.io/' # STAGE (old "DCP")
# creds = '/Users/christopher/Downloads/stage-credentials.json'

api = 'https://vpodc.org/' #
profile = 'vpodc'
creds = '/Users/christopher/Downloads/vpodc-credentials.json'

api = 'https://qa-brain.planx-pla.net/'
profile = 'qa-brain'
creds = '/Users/christopher/Downloads/qa-credentials.json'


sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/')
from expansion import Gen3Expansion
from migration import Gen3Migration


auth = Gen3Auth(api, refresh_file=creds)

#!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py # get the latest beta version in GitHub
#%run ./submission.py # run the latest version in GitHub
sub = Gen3Submission(api, auth) # Initialize an instance this class, using your creds in 'auth'


%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py # Download additional functions in Gen3Expansion class
exp = Gen3Expansion(api, auth) # Initialize an instance, using it like exp.get_project_tsvs()

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

# Get the gen3sdk and expansion functions
import os

home_dir = "/home/jovyan/pd/"
gen3_dir = "{}.gen3".format(home_dir)
!mkdir -p ${gen3_dir}
os.chdir(gen3_dir)

!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/auth.py
!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py

!curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*osx |  cut -d '"' -f 4 | wget -qi -
!unzip dataclient_osx.zip
!rm dataclient_osx.zip



%run ./auth.py
%run ./submission.py
%run ./expansion.py

api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
profile = 'dcf'
creds = '/Users/christopher/Downloads/dcf-credentials.json'

auth = Gen3Auth(api, refresh_file=creds)
exp = Gen3Expansion(api, auth)
sub = Gen3Submission(api, auth)

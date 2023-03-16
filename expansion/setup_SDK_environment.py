# new way:
import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

####################################################################################
####################################################################################
####################################################################################
### Run Locally in "ipython" (or a Jupyter Notebook)
# some random comment
## Set gen3-client profile name, api endpoint of the data commons, and path to credentials file
profile = 'bc'
api = 'https://data.braincommons.org/' # BRAIN Commons
creds = '/Users/christopher/Downloads/bc-credentials.json'

profile = 'qa-brain'
api = 'https://qa-brain.planx-pla.net/'
cred = '/Users/christopher/Downloads/qa-credentials.json'

profile = 'gen3qa-brain'
api = 'https://gen3qa.braincommons.org/' # Gen3QA for BRAIN Commons
cred = '/Users/christopher/Downloads/gen3qa-brain-credentials.json'

profile = 'staging'
api = 'https://data-staging.braincommons.org/' # BRAIN Commons
cred = '/Users/christopher/Downloads/brain-staging-credentials.json'

profile = 'bpa'
api = 'https://data.bloodpac.org/' # BloodPAC
cred = '/Users/christopher/Downloads/bpa-credentials.json'

profile = 'ndh'
api = 'https://niaid.bionimbus.org/'
cred = '/Users/christopher/Downloads/ndh-credentials.json'

profile = 'vpodc'
api = 'https://vpodc.org/' #
cred = '/Users/christopher/Downloads/vpodc-credentials.json'

profile = 'dcf'
api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
cred = '/Users/christopher/Downloads/dcf-credentials.json'

profile = 'stage'
api = 'https://gen3.datastage.io/' # STAGE (old "DCP")
cred = '/Users/christopher/Downloads/stage-credentials.json'

profile = 'acct'
api = 'https://acct.bionimbus.org/'
cred = '/Users/christopher/Downloads/acct-credentials.json'

profile = 'genomel'
api = 'https://genomel.bionimbus.org/'
cred = '/Users/christopher/Downloads/genomel-credentials.json'


# api = 'https://dcf-interop.kidsfirstdrc.org/' #Kids First


# Import the Gen3 Python SDK from my local copy of it downloaded from GitHub
# use this command to get the sdk: `git clone git@github.com:cgmeyer/gen3sdk-python.git`
import pandas as pd
import sys

git_dir='/Users/christopher/Documents/GitHub'
sdk_dir='/cgmeyer/gen3sdk-python'
sys.path.insert(1, '{}{}'.format(git_dir,sdk_dir))
from expansion.expansion import Gen3Expansion
from migration.migration import Gen3Migration
from gen3.submission import Gen3Submission
from gen3.auth import Gen3Auth
auth = Gen3Auth(api, refresh_file=creds)
sub = Gen3Submission(api, auth) # Initialize an instance this class, using your creds in 'auth'

# run my local working copy
%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion/expansion.py # Some additional functions in Gen3Expansion class
exp = Gen3Expansion(sub, api, auth) # Initialize an instance, using its functions like exp.get_project_tsvs()

# download the sdk files directly from GitHub and run in ipython:
#!wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py
#%run ./submission.py
#!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py
#%run ./expansion.py
#!wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/migration/migration.py
#%run ./migration.py

# Download and configure MacOsX gen3-client
mkdir -p ~/.gen3
export PATH=$PATH:~/.gen3
curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*osx |  cut -d '"' -f 4 | wget -qi -
unzip dataclient_osx.zip
mv gen3-client ~/.gen3
rm dataclient_osx.zip



# Now configure my gen3-client profile:
client = 'gen3-client'
config_cmd = "{} configure --profile={} --apiendpoint={} --cred={}".format(client, profile, api, creds)
auth_cmd = "{} auth --profile={}".format(client, profile)
try:
    output = subprocess.check_output(config_cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
    output = subprocess.check_output(auth_cmd, stderr=subprocess.STDOUT, shell=True).decode('UTF-8')
    print(output)
except Exception as e:
    output = e.output.decode('UTF-8')
    print("ERROR:" + output)
























############################################################################################
############################################################################################
# This version is for the Gen3 Workspace JupyterHub:
# Run this in a Python 3 Jupyter notebook.

# To install packages from the terminal, set proxies:
# export http_proxy="http://cloud-proxy.internal.io:3128";
# export https_proxy="http://cloud-proxy.internal.io:3128"
# export no_proxy="localhost,127.0.0.1,*.internal.io"

# Get the gen3sdk and expansion functions
import os
import requests, json, fnmatch, os.path, sys, subprocess, glob, ntpath, copy, re
import pandas as pd
from pandas.io.json import json_normalize
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# make a directory for the gen3 sdk/client in the 'pd' (persistent drive)
home_dir = "/home/jovyan/pd"
gen3_dir = "{}/gen3".format(home_dir)
os.system('mkdir -p {}'.format(gen3_dir))
os.chdir(gen3_dir)

# Get the latest gen3 SDK files
# Get the latest gen3 SDK files
os.system("wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/submission.py -O {}/submission.py".format(gen3_dir))
os.system("wget https://raw.githubusercontent.com/uc-cdis/gen3sdk-python/master/gen3/auth.py -O {}/auth.py".format(gen3_dir))
os.system("wget https://raw.githubusercontent.com/cgmeyer/gen3sdk-python/master/expansion/expansion.py -O {}/expansion.py".format(gen3_dir))

# Run the SDK files
%run ./auth.py
%run ./submission.py
%run ./expansion.py

# Set gen3-client profile name, data commons API URL, and credentials location.
profile = 'dcf'
api = 'https://nci-crdc-demo.datacommons.io/' # DCF  SAndbox Commons
cred = '/Users/christopher/Downloads/dcf-credentials.json'

# Create instances of the gen3sdk classes for a data commons API and matching credentials file.
auth = Gen3Auth(api, refresh_file=cred)
exp = Gen3Expansion(api, auth)
sub = Gen3Submission(api, auth)

# Get the latest gen3-client
!curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget -qi -
!unzip dataclient_linux.zip
!rm dataclient_linux.zip
client = "{}/gen3-client".format(gen3_dir)

curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget -qi -
unzip dataclient_linux.zip
rm dataclient_linux.zip
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

export http_proxy=http://cloud-proxy.internal.io:3128
export https_proxy=http://cloud-proxy.internal.io:3128

curl https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget -qi -
#curl -k https://api.github.com/repos/uc-cdis/cdis-data-client/releases/latest | grep browser_download_url.*linux |  cut -d '"' -f 4 | wget --no-check-certificate -qi -

unzip dataclient_linux.zip
rm dataclient_linux.zip

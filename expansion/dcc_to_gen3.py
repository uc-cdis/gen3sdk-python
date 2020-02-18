"""
This script can be used to convert a file download manifest from https://dcc.icgc.org/
to a manifest that the gen3-client (https://github.com/uc-cdis/cdis-data-client) can use.

Example:
python delete_uploaded_files.py -a https://nci-crdc-demo.datacommons.io/ -u user@datacommons.org -c ~/Downloads/demo-credentials.json

Arguments:
    -a or --api: The URL for the data commons the file was uploaded to.
    -u or --user: The uploader's login email (or user ID).
    -c or --creds: The location of the credentials.json downloaded from the data commons portal (Windmill's "Profile" page).
"""
import json, requests, os, argparse, re
import pandas as pd

global locs, irecs, args, token, all_records

def parse_args():
    parser = argparse.ArgumentParser(description="Generate a gen3-client manifest from a DCC manifest by retrieving GUIDs for files from indexd.")
    parser.add_argument("-m", "--manifest", required=True, help="The data commons URL.",default="dcc_manifest.pdc.1581529402015.sh")
    parser.add_argument("-a", "--api", required=False, help="The data commons URL.",default="https://icgc.bionimbus.org/")
    parser.add_argument("-i", "--indexd", required=False, help="If a file already exists with the data commons indexd records, provide the path to that file.",default=False) # default="icgc.bionimbus.org_indexd_records.txt"
    parser.add_argument("-c", "--cred", required=False, help="The location of the credentails.json file containing the user's API keys downloaded from the /profile page of the commons.", default="/Users/christopher/Downloads/icgc-credentials.json")
    args = parser.parse_args()
    return args

def get_token():
    """ get your temporary access token using your credentials downloaded from the data portal
    """
    with open (args.cred, 'r') as f:
        credentials = json.load(f)
    token_url = "{}/user/credentials/api/access_token".format(args.api)
    resp = requests.post(token_url, json=credentials)
    if (resp.status_code != 200):
        raise(Exception(resp.reason))
    token = resp.json()['access_token']
    return token

    # trouble-shooting
    # cred = '/Users/christopher/Downloads/icgc-credentials.json'
    # api = 'https://icgc.bionimbus.org/'
    # with open (cred, 'r') as f:
    #     credentials = json.load(f)
    # token_url = "{}/user/credentials/api/access_token".format(api)
    # resp = requests.post(token_url, json=credentials)
    # if (resp.status_code != 200):
    #     raise(Exception(resp.reason))
    # token = resp.json()['access_token']


def get_indexd(outfile=True):
    """ get all the records in indexd
    """
    headers = {'Authorization': 'bearer ' + get_token()}
    all_records = []
    indexd_url = "{}/index/index".format(args.api)
    response = requests.get(indexd_url, headers=headers) #response = requests.get(indexd_url, auth=auth)
    records = response.json().get("records")
    all_records.extend(records)
    print("\tRetrieved {} records from indexd.".format(len(all_records)))

    previous_did = None
    start_did = records[-1].get("did")

    while start_did != previous_did:
        previous_did = start_did
        next_url = "{}?start={}".format(indexd_url,start_did)
        response = requests.get(next_url, headers=headers) #response = requests.get(next_url, auth=auth)
        records = response.json().get("records")
        all_records.extend(records)
        print("\tRetrieved {} records from indexd.".format(len(all_records)))
        if records:
            start_did = response.json().get("records")[-1].get("did")
    if outfile:
        dc_regex = re.compile(r'https:\/\/(.+)\/')
        dc = dc_regex.match(args.api).groups()[0]
        outname = "{}_indexd_records.txt".format(dc)
        with open(outname, 'w') as output:
            output.write(json.dumps(all_records))
    return all_records

def read_dcc_manifest():

    try:
        with open(args.manifest, 'r') as dcc_file:
            dcc_lines = [line.rstrip() for line in dcc_file]
    except Exception as e:
        print("Couldn't read the manifest file '{}': {} ".format(args.indexd,e))

    dcc_regex = re.compile(r'^.+cp (s3.+) \.$')
    dcc_files = [dcc_regex.match(i).groups()[0] for i in dcc_lines if dcc_regex.match(i)]

    return dcc_files

# # manifest = 'dcc_manifest.sh'
#         with open(manifest, 'r') as dcc_file:
#             dcc_files = [line.rstrip() for line in dcc_file]

def write_manifest():
    """ write the gen3-client manifest
    """
    #locs = {irec['urls'][0]: irec for irec in irecs}
    count = 0
    total = len(dcc_files)
    mname = "gen3_manifest_{}.json".format(args.manifest)

    with open(mname, 'w') as manifest:
        manifest.write('[')
        for loc in dcc_files:
            count+=1

            fsize = locs[loc]['size']
            object_id = locs[loc]['did']
            #print("\t{} file_size: {}, object_id: {}".format(loc,fsize,object_id))

            manifest.write('\n\t{')
            manifest.write('"object_id": "{}", '.format(object_id))
            manifest.write('"location": "{}", '.format(loc))
            manifest.write('"size": "{}"'.format(fsize))

            if count == len(dcc_files):
                manifest.write('  }]')
            else:
                manifest.write('  },')

            print("\t{} ({}/{})".format(object_id,count,total))

        print("\tDone ({}/{}).".format(count,total))
        print("\tManifest written to file: {}".format(mname))


if __name__ == "__main__":

    args = parse_args()

    if args.indexd:
        try:
            print("Reading provided indexd file '{}'".format(args.indexd))
            with open(args.indexd, 'r') as indexd_file:
                itxt = indexd_file.readline()
                irecs = json.loads(itxt)
                print("\t{} file records were found in the indexd file.".format(len(irecs)))
        except Exception as e:
            print("Couldn't load the file '{}': {} ".format(args.indexd,e))
    else:
        print("No filename provided for indexd records, fetching file index records from {}.".format(args.api))
        irecs = get_indexd(args.api)
        print("\t{} total records retrieved from {}.".format(len(irecs),args.api))

    locs = {irec['urls'][0]: irec for irec in irecs}

    print("Reading the dcc manifest: {}".format(args.manifest))
    dcc_files = read_dcc_manifest()
    print("\tFound {} files in this dcc manifest.".format(len(dcc_files)))

    print("Writing the gen3-client manifest.")
    write_manifest()






# python dcc_to_gen3.py -m dcc_manifest.sh
# python dcc_to_gen3.py -m dcc_manifest.sh -i icgc.bionimbus.org_indexd_records.txt


# indexd = 'icgc.bionimbus.org_indexd_records.txt'
# with open(indexd, 'r') as indexd_file:
#     itxt = indexd_file.readline()
#     irecs = json.loads(itxt)

# gen3-client download-multiple --profile=icgc --manifest=gen3_manifest_dcc_manifest.sh.json --no-prompt --download-path='pcawg_files'

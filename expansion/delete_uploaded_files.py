"""
This script can be used to delete files uploaded with the gen3-client that are not yet "mapped" to a node in a project (i.e., no records exist for the file yet in the Postgres database).
When successful, the script deletes the indexd record for the files and also deletes the file from the cloud location using the "fence" service endpoint.

Example:
python delete_uploaded_files.py -a https://nci-crdc-demo.datacommons.io/ -u user@datacommons.org -c ~/Downloads/demo-credentials.json

Arguments:
    -a or --api: The URL for the data commons the file was uploaded to.
    -u or --user: The uploader's login email (or user ID).
    -c or --creds: The location of the credentials.json downloaded from the data commons portal (Windmill's "Profile" page).
"""
import json, requests, os, argparse

def parse_args():
    global args
    parser = argparse.ArgumentParser(description="Delete unmapped files uploaded with the gen3-client.")
    parser.add_argument("-a", "--api", required=True, help="The data commons URL.",default="https://nci-crdc-demo.datacommons.io/")
    parser.add_argument("-u", "--user", required=True, help="The user's login email.",default="cgmeyer@uchicago.edu")
    parser.add_argument("-c", "--creds", required=True, help="The location of the credentails.json file containing the user's API keys downloaded from the /profile page of the commons.", default="~/Downloads/demo-credentials.json")
    args = parser.parse_args()
    return args

def get_token():
    global token
    with open (args.creds, 'r') as f:
        credentials = json.load(f)
    token_url = "{}/user/credentials/api/access_token".format(args.api)
    resp = requests.post(token_url, json=credentials)
    if (resp.status_code != 200):
        raise(Exception(resp.reason))
    token = resp.json()['access_token']
    return token

def uploader_index():
    """Get records from indexd of the files uploaded by a particular user.

    Args:
        uploader (str): The uploader's data commons login email.

    Examples:
        This returns all records of files that I uploaded to indexd.

        >>> Gen3Submission.submit_file(uploader="cgmeyer@uchicago.edu")
        #data.bloodpac.org/index/index/?limit=1024&acl=null&uploader=cgmeyer@uchicago.edu
    """
    global guids
    headers = {'Authorization': 'bearer ' + get_token()}
    index_url = "{}/index/index/?limit=1024&uploader={}".format(args.api,args.user)
    try:
        response = requests.get(
            index_url,
            headers=headers
        ).text
    except requests.exceptions.ConnectionError as e:
        print(e)

    try:
        data = json.loads(response)
    except ValueError as e:
        print(response)
        print(str(e))
        raise Gen3Error("Unable to parse indexd response as JSON!")

    records = data['records']
    guids = []

    if records is None:
        print("No records in the index for uploader {}.".format(args.user))

    else:
        for record in records:
            guids.append(record['did'])
        return guids

def delete_uploaded_files(guids):
    """ Deletes all locations of a stored data file and remove its record from indexd.
    """
    headers = {'Authorization': 'bearer ' + get_token()}
    if isinstance(guids, str):
        guids = [guids]

    if not isinstance(guids, list):
        raise Gen3Error("Please, supply GUIDs as a list.")

    for guid in guids:
        fence_url = "{}/user/data/".format(args.api)
        try:
            response = requests.delete(fence_url + guid,headers=headers)
        except requests.exceptions.ConnectionError as e:
            raise Gen3Error(e)

        if (response.status_code == 204):
            print("Successfully deleted GUID {}".format(guid))
        else:
            print("Error deleting GUID {}:".format(guid))
            print(response.reason)

if __name__ == "__main__":
    args = parse_args()
    guids = uploader_index()
    if len(guids)!=0:
        print("Found the following guids for uploader {}: {}".format(args.user,guids))
        delete_uploaded_files(guids)
    else:
        print("No GUIDs found in the indexd database for uploader: {}".format(args.user))

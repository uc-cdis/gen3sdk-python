import json, requests, os, argparse

                # if __name__ == "__main__":
                #     #guid = "be022658-06a2-4738-a58f-765804ab2254"
                #     creds = '/Users/christopher/Downloads/vpodc-credentials.json'
                #     api = 'https://vpodc.org/'
                #     logfile = '/Users/christopher/.gen3/OLD_vpodc_succeeded_log.json'
                #     clear_log(logfile,api)

def parse_args():
    global args
    parser = argparse.ArgumentParser(description="Delete unmapped files uploaded with the gen3-client.")
    parser.add_argument("-a", "--api", required=True, help="The data commons URL.",default="https://nci-crdc-demo.datacommons.io/")
    parser.add_argument("-u", "--user", required=True, help="The user's login email.",default="cgmeyer@uchicago.edu")
    parser.add_argument("-c", "--creds", required=True, help="The location of the credentails.json file containing the user's API keys downloaded from the /profile page of the commons.", default="~/Downloads/demo-credentials.json")
    parser.add_argument("-d", "--dry_run", required=False, help="Test the execution of the commands without actually deleting the records. Substitues all DELETE requests with GET requests.", default=False)
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

    if records is None:
        print("No records in the index for uploader {}.".format(args.user))

    else:
        guids = []
        for record in records:
            guids.append(record['did'])
        return guids

def delete_uploaded_files(guids):
    """
    DELETE http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/fence/master/openapis/swagger.yaml#/data/delete_data__file_id_
    Deletes all locations of a stored data file and remove its record from indexd.
    After a user uploads a data file and it is registered in indexd,
    but before it is mapped into the graph via metadata submission,
    this endpoint will delete the file from its storage locations (saved in the record in indexd)
    and delete the record in indexd.

    Args:
        guids (list): The list of GUIDs to delete.

    Examples:
        >>> Gen3Expansion.delete_uploaded_files(guids="dg.7519/fd0d91e0-87a6-4627-80b4-50d98614c560")
        >>> Gen3Expansion.delete_uploaded_files(guids=["dg.7519/fd0d91e0-87a6-4627-80b4-50d98614c560","dg.7519/bc78b25d-6203-4d5f-9257-cc6bba3fc34f"])
    """
    headers = {'Authorization': 'bearer ' + get_token()}
    if isinstance(guids, str):
        guids = [guids]

    if not isinstance(guids, list):
        raise Gen3Error("Please, supply GUIDs as a list.")

    for guid in guids:
        fence_url = "{}/user/data/".format(args.api)
        try:
            if args.dry_run:
                response = requests.get(fence_url + guid,headers=headers)
            else:
                response = requests.get(fence_url + guid,headers=headers)
        except requests.exceptions.ConnectionError as e:
            raise Gen3Error(e)

        if (response.status_code == 204):
            if args.dry_run:
                print("Successful dry run for deleting GUID {}".format(guid))
            else:
                print("Successfully deleted GUID {}".format(guid))
        else:
            print("Error deleting GUID {}:".format(guid))
            print(response.reason)

# def delete_uploaded_file(guid):
#     headers = {'Authorization': 'bearer ' + get_token()}
#
#     fence_url = "/user/data/".format(args.api)
#     response = requests.delete(
#         fence_url + guid,
#         headers=headers
#     )
#     if (response.status_code == 204):
#         print("Successfully deleted GUID {}".format(guid))
#     else:
#         print("Error deleting GUID {}:".format(guid))
#         print(response.reason)
#
# def get_guids_for_filenames(file_names):
#     """Get GUIDs for a list of file_names"""
#     global guids
#     headers = {'Authorization': 'bearer ' + get_token()}
#     if isinstance(file_names, str):
#         file_names = [file_names]
#     if not isinstance(file_names,list):
#         print("Please provide one or a list of data file file_names: get_guid_for_filename\(file_names=file_name_list\)")
#     guids = {}
#     for file_name in file_names:
#         index_url = args.api + '/index/index/?file_name=' + file_name
#         output = requests.get(index_url, headers=headers).text
#         index_record = json.loads(output)
#         if len(index_record['records']) > 0:
#             guid = index_record['records'][0]['did']
#             guids[file_name] = guid
#     return guids
if __name__ == "__main__":
    args = parse_args()
    guids = uploader_index()
    print("Found the following guids for uploader {}: {}".format(args.user,guids))
    delete_uploaded_files(guids)

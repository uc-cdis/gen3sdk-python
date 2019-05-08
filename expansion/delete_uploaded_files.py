import requests, json

def get_token():
    with open ('./credentials.json', 'r') as f:
        creds = json.load(f)
    token_url = 'https://cvbcommons.org/user/credentials/api/access_token'
    resp = requests.post(token_url, json=creds)
    if (resp.status_code != 200):
        raise(Exception(resp.reason))
    token = resp.json()['access_token']
    return token


def delete_uploaded_file(guid):
    headers = {'Authorization': 'bearer ' + get_token()}

    fence_url = 'https://cvbcommons.org/user/data/'
    response = requests.delete(
        fence_url + guid,
        headers=headers
    )
    if (response.status_code == 204):
        print("Successfully deleted GUID {}".format(guid))
    else:
        print("Error deleting GUID {}:".format(guid))
        print(response.reason)


if __name__ == "__main__":
    guid = "be022658-06a2-4738-a58f-765804ab2254"
    delete_uploaded_file(guid)

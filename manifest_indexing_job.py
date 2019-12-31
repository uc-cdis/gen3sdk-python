"""
Module for indexing actions using sower job dispatcher

Attributes:
"""

from gen3.tools import manifest_indexing

def _download(url, filename):
    r = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(r.content)

if __name__ == "__main__":
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]
    
    input_data_json = json.loads(input_data)
    
    with open("/indexd-creds.json") as indexing_creds_file:
        indexing_creds = json.load(indexing_creds_file)
    
    auth = (indexing_creds.get("indexd_user", "gdcapi"), indexing_creds["indexd_password"])

    filepath = "./manifest_tmp.tsv"
    _download_file(input_data_json["URL"], filepath)

    print("Start to index the manifest ...")

    manifest_indexing(filepath, "{}/index/index".format(hostname), input_data_json.get("thread_nums", 1), input_data_json.get("prefix"), input_data_json.get("replace_urls"))


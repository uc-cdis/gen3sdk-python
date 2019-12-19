import csv
from functools import partial
import logging
from multiprocessing.dummy import Pool as ThreadPool
import uuid

import indexclient.client as client


# Pre-defined supported column names
GUID = ["guid", "uuid"]
FILENAME = ["filename", "file_name"]
SIZE = ["size", "filesize", "file_size"]
MD5 = ["md5", "md5_hash", "hash"]
ACLS = ["acl", "acls"]
URLS = ["url", "urls"]

def _get_and_verify_fileinfos_from_tsv_manifest(manifest_file, dem="\t"):
    """
    get and verify file infos from tsv manifest

    Args:
        manifest_file(str): the path to the input manifest
        dem(str): delimiter
    
    Returns:
        list(dict): list of file info
        [
            {
                "GUID": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
    """
    files = []
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=dem)
        for row in csvReader:
            for key in row.keys():
                standardized_key = None
                if key in GUID:
                    standardized_key = "GUID"
                elif key in FILENAME:
                    standardized_key = "filename"
                elif key in MD5:
                    standardized_key = "md5"
                elif key in ACLS:
                    standardized_key = "acl"
                elif key in URLS:
                    standardized_key = "url"

                if standardized_key:
                    row[standardized_key] = row[key].strip())
                elif key in SIZE:
                    row["size"] = int(row["size"].strip())

            files.append(row)

    return files


def _index_record(prefix, indexclient, replace_urls, fi):
    """
    Index single file

    Args:
        prefix(str): GUID prefix
        indexclient(IndexClient): indexd client
        replace_urls(bool): replace urls or not
        fi(dict): file info 

    Returns:
        None

    """
    try:
        urls = fi.get("url", "").strip().split(" ")

        if fi.get("acl", "").strip().lower() in {"[u'open']", "['open']"}:
            acl = ["*"]
        else:
            acl = [
                element.strip().replace("'", "")
                for element in fi.get("acl", "").strip()[1:-1].split(",")
            ]

        uuid = uuid.uuid4() if not fi.get("GUID") else fi.get("GUID")

        doc = None
        if  fi.get("GUID"):
            doc = indexclient.get(prefix + uuid)
    
        if doc is not None:
            need_updat  e = False

            for url in urls:
                if not replace_urls and url not in doc.urls:
                    doc.urls.append(url)
                    need_update = True

            if replace_urls and set(urls) != set(doc.urls):
                doc.urls = urls
                need_update = True

                # indexd doesn't like when records have metadata for non-existing
                # urls
                new_urls_metadata = copy.deepcopy(doc.urls_metadata)
                for url, metadata in doc.urls_metadata.items():
                    if url not in urls:
                        del new_urls_metadata[url]

                doc.urls_metadata = new_urls_metadata

            if set(doc.acl) != set(acl):
                doc.acl = acl
                need_update = True

            if need_update:
                doc.patch()
        else:
            doc = indexclient.create(
                did=prefix + fi.get("GUID", "").strip(),
                hashes={"md5": fi.get("md5", "").strip()},
                size=fi.get("size", 0),
                acl=acl,
                urls=urls,
            )


    except Exception as e:
        # Don't break for any reason
        logging.error(
            "Can not update/create an indexd record with uuid {}. Detail {}".format(
                fi.get("GUID"), e
            )
        )


def manifest_indexing(manifest, common_url, thread_num, auth=None, prefix=None, replace_urls=False):
    """
    Loop through all the files in the manifest, update/create records in indexd
    update indexd if the url is not in the record url list or acl has changed

    Args:
        manifest(str): path to the manifest
        common_url(str): common url
        thread_num(int): number of threads for indexing
        auth(Gen3Auth): Gen3 auth
        prefix(str): GUID prefix

    """
    indexclient = client.IndexClient(
        common_url,
        "v0",
        auth=auth,
    )

    try:
        files = _get_and_verify_fileinfos_from_tsv_manifest(manifest)
    except Exception as e:
        logging.error("Can not read {}. Detail {}".format(manifest, e))
        return

    prefix = prefix + "/" if prefix else ""
    pool = ThreadPool(thread_num)

    part_func = partial(_index_record, prefix, indexclient, replace_urls)
    try:
        pool.map_async(part_func, files).get(9999999)
    except KeyboardInterrupt:
        pool.terminate()

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

# if __name__ == "__main__":
#     manifest_indexing("/Users/giangbui/Projects/indexd_utils/test.tsv", "https://giangb.planx-pla.net/index/index", 1, auth=None, prefix=None, replace_urls=False)
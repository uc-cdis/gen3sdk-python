import csv
from functools import partial
import logging
from multiprocessing.dummy import Pool as ThreadPool
import indexclient.client as client


def _index_record(prefix, indexclient, replace_urls, fi):
    try:
        urls = fi.get("url").split(" ")

        if fi.get("acl").lower() in {"[u'open']", "['open']"}:
            acl = ["*"]
        else:
            acl = [
                element.strip().replace("'", "")
                for element in fi.get("acl")[1:-1].split(",")
            ]

        doc = indexclient.get(prefix + fi.get("GUID"))
        if doc is not None:
            need_update = False

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
                did=prefix + fi.get("GUID"),
                hashes={"md5": fi.get("md5")},
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


def _get_fileinfos_from_tsv_manifest(manifest_file, dem="\t"):
    """
    get file info from tsv manifest
    """
    files = []
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=dem)
        for row in csvReader:
            row["size"] = int(row["size"])
            files.append(row)

    return files


def manifest_indexing(manifest, common_url, thread_num, auth=None, prefix=None, replace_urls=False):
    """
    Loop through all the files in the manifest, update/create records in indexd
    update indexd if the url is not in the record url list or acl has changed
    """
    indexclient = client.IndexClient(
        common_url,
        "v0",
        auth=auth,
    )

    try:
        files = _get_fileinfos_from_tsv_manifest(manifest)
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
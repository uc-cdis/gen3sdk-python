"""
Module for indexing object files in a manifest (against indexd's API).

The default manifest format created is a Tab-Separated Value file (tsv)
with rows for every record.

Fields that are lists (like acl, authz, and urls) separate the values with commas or spaces
See the Attributes session for supported column names.

All supported formats of acl, authz and url fields are shown in the below example.

guid    md5 size    acl authz   url
255e396f-f1f8-11e9-9a07-0a80fada099c    473d83400bc1bc9dc635e334faddf33c    363455714   ['Open']    [s3://pdcdatastore/test1.raw]
255e396f-f1f8-11e9-9a07-0a80fada098c    473d83400bc1bc9dc635e334faddd33c    343434344   Open    s3://pdcdatastore/test2.raw
255e396f-f1f8-11e9-9a07-0a80fada097c    473d83400bc1bc9dc635e334fadd433c    543434443   phs0001 phs0002 s3://pdcdatastore/test3.raw
255e396f-f1f8-11e9-9a07-0a80fada096c    473d83400bc1bc9dc635e334fadd433c    363455714   ['phs0001', 'phs0002']  ['s3://pdcdatastore/test4.raw']
255e396f-f1f8-11e9-9a07-0a80fada010c    473d83400bc1bc9dc635e334fadde33c    363455714   ['Open']    s3://pdcdatastore/test5.raw

Attributes:
    CURRENT_DIR (str): directory this file is in
    GUID (list(string)): supported file id column names
    SIZE (list(string)): supported file size column names
    MD5 (list(string)): supported md5 hash column names
    ACLS (list(string)): supported acl column names
    URLS (list(string)): supported url column names
    AUTHZ (list(string)): supported authz column names
    PREV_GUID (list(string)): supported previous guid column names

Usages:
    python index_manifest.py --commons_url https://giangb.planx-pla.net  --manifest_file path_to_manifest --auth "admin,admin" --replace_urls False --thread_num 10
    python index_manifest.py --commons_url https://giangb.planx-pla.net  --manifest_file path_to_manifest --api_key ./credentials.json --replace_urls False --thread_num 10
"""
import os
import csv
import click
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
import threading
import copy
import sys
import traceback

from gen3.index import Gen3Index
from gen3.auth import Gen3Auth
from gen3.metadata import Gen3Metadata
from gen3.tools.utils import (
    GUID_STANDARD_KEY,
    FILENAME_STANDARD_KEY,
    SIZE_STANDARD_KEY,
    MD5_STANDARD_KEY,
    ACL_STANDARD_KEY,
    URLS_STANDARD_KEY,
    AUTHZ_STANDARD_KEY,
    PREV_GUID_STANDARD_KEY,
)
from gen3.utils import (
    _standardize_str,
    get_urls,
)
from gen3.tools.utils import get_and_verify_fileinfos_from_manifest
import indexclient.client as client
from indexclient.client import Document
from cdislogging import get_logger


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

logging = get_logger("__name__")


class ThreadControl(object):
    """
    Class for thread synchronization
    """

    def __init__(self, processed_files=0, num_total_files=0):
        self.mutexLock = threading.Lock()
        self.num_processed_files = processed_files
        self.num_total_files = num_total_files


def _write_csv(filename, files, fieldnames=None):
    """
    write to csv file

    Args:
        filename(str): file name
        files(list(dict)): list of file info
        [
            {
                "guid": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        fieldnames(list(str)): list of column names

    Returns:
        None
    """

    if not files:
        return None
    fieldnames = fieldnames or files[0].keys()
    with open(filename, mode="w") as outfile:
        writer = csv.DictWriter(outfile, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()

        for f in files:
            writer.writerow(f)

    return filename


def _index_record(
    indexclient,
    mds,
    replace_urls,
    thread_control,
    submit_additional_metadata_columns,
    force_metadata_columns_even_if_empty,
    fi,
):
    """
    Index a single file, and submit additional metadata to the metadata service if provided

    Args:
        indexclient(IndexClient): indexd client
        mds(Gen3Metadata): optional Gen3Metadata instance
        replace_urls(bool): replace urls or not
        thread_num(int): number of threads for indexing
        submit_additional_metadata_columns(bool): whether to submit additional metadata to the metadata service
        force_metadata_columns_even_if_empty (bool): see description in calling function
        fi(dict): file info

    Returns:
        None

    """
    index_success = True
    try:
        urls = (
            get_urls(fi[URLS_STANDARD_KEY])
            if URLS_STANDARD_KEY in fi
            and fi[URLS_STANDARD_KEY] != "[]"
            and fi[URLS_STANDARD_KEY]
            else []
        )
        authz = (
            [
                element.strip().replace("'", "").replace('"', "").replace("%20", " ")
                for element in _standardize_str(fi[AUTHZ_STANDARD_KEY])
                .strip()
                .lstrip("[")
                .rstrip("]")
                .split(" ")
            ]
            if AUTHZ_STANDARD_KEY in fi
            and fi[AUTHZ_STANDARD_KEY] != "[]"
            and fi[AUTHZ_STANDARD_KEY]
            else []
        )

        if ACL_STANDARD_KEY in fi:
            if fi[ACL_STANDARD_KEY].strip().lower() in {"[u'open']", "['open']"}:
                acl = ["*"]
            else:
                acl = (
                    [
                        element.strip()
                        .replace("'", "")
                        .replace('"', "")
                        .replace("%20", " ")
                        for element in _standardize_str(fi[ACL_STANDARD_KEY])
                        .strip()
                        .lstrip("[")
                        .rstrip("]")
                        .split(" ")
                    ]
                    if ACL_STANDARD_KEY in fi
                    and fi[ACL_STANDARD_KEY] != "[]"
                    and fi[ACL_STANDARD_KEY]
                    else []
                )
        else:
            acl = []

        if FILENAME_STANDARD_KEY in fi:
            file_name = _standardize_str(fi[FILENAME_STANDARD_KEY])
        else:
            file_name = ""

        if fi.get(PREV_GUID_STANDARD_KEY):
            prev_guid = fi[PREV_GUID_STANDARD_KEY]
        else:
            prev_guid = None

        doc = None

        if fi.get(GUID_STANDARD_KEY):
            doc = indexclient.get(fi[GUID_STANDARD_KEY])

        if doc is not None:
            if doc.size != fi.get(SIZE_STANDARD_KEY) or doc.hashes.get(
                MD5_STANDARD_KEY
            ) != fi.get(MD5_STANDARD_KEY):
                logging.error(
                    "The guid {} with different size/hash already exists. Can not index it without getting a new guid".format(
                        fi.get(GUID_STANDARD_KEY)
                    )
                )
            else:
                need_update = False

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

                elif not replace_urls:
                    for url in urls:
                        if url not in doc.urls:
                            doc.urls.append(url)
                            need_update = True

                if set(doc.acl) != set(acl):
                    doc.acl = acl
                    need_update = True

                if set(doc.authz) != set(authz):
                    doc.authz = authz
                    need_update = True

                if doc.file_name != file_name:
                    doc.file_name = file_name
                    need_update = True

                if need_update:
                    logging.info(f"updating {doc.did} to: {doc.to_json()}")
                    doc.patch()
        else:
            if fi.get(GUID_STANDARD_KEY):
                guid = fi.get(GUID_STANDARD_KEY, "").strip()
            else:
                guid = None

            record = {
                "did": guid,
                "hashes": {MD5_STANDARD_KEY: fi.get(MD5_STANDARD_KEY, "").strip()},
                SIZE_STANDARD_KEY: fi.get(SIZE_STANDARD_KEY, 0),
                ACL_STANDARD_KEY: acl,
                AUTHZ_STANDARD_KEY: authz,
                URLS_STANDARD_KEY: urls,
                FILENAME_STANDARD_KEY: file_name,
            }

            if prev_guid:
                logging.info(f"creating new version of {prev_guid}: {record}")

                # indexd exports a "form" field that gets populated in indexclient.create,
                # but not indexclient.add_version, need to add manually here
                record.update({"form": "object"})

                # to generate new GUID, new version indexd API expects body to not
                # contain "did", rather than have it be None or ""
                new_guid = record["did"]
                if not record["did"]:
                    del record["did"]
                    new_guid = None

                new_doc = Document(client=None, did=new_guid, json=record)

                # TODO: in the case where a new GUID *is* provided AND a version with that
                #       guid already exists AND you run this, it's gonna throw an error.
                #       We need to gracefully handle the error from indexd. there will
                #       be a conflict where a version with this GUID already exists...
                #       but if we can verify that the version has the correct values
                #       for all the fields, we can effectively ignore this error and continue
                doc = indexclient.add_version(current_did=prev_guid, new_doc=new_doc)
            else:
                logging.info(f"creating: {record}")
                doc = indexclient.create(**record)

            fi[GUID_STANDARD_KEY] = doc.did

    except Exception as e:
        # Don't break for any reason
        index_success = False
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        logging.error(
            "Can not update/create an indexd record with guid {}. Detail: {}".format(
                fi.get(GUID_STANDARD_KEY), e
            )
        )

    # submit additional metadata to the metadata service
    if submit_additional_metadata_columns and index_success:
        try:
            if not mds:
                raise Exception(
                    "Can not submit to the metadata service when using indexd basic auth"
                )
            metadata = mds._prepare_metadata(
                fi,
                doc,
                force_metadata_columns_even_if_empty=force_metadata_columns_even_if_empty,
            )
            if metadata:
                mds.create(guid=doc.did, metadata=metadata, overwrite=True)
        except Exception as e:
            # Don't break, but delete indexd record
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            logging.error(
                "Can not create package metadata for guid {}. Deleting indexd record. Detail: {}".format(
                    fi[GUID_STANDARD_KEY], e
                )
            )
            try:
                doc.delete()
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                logging.error(
                    "Cannot delete indexd record with {}. Detail: {}".format(
                        fi[GUID_STANDARD_KEY], e
                    )
                )

    thread_control.mutexLock.acquire()
    thread_control.num_processed_files += 1
    if (thread_control.num_processed_files * 10) % thread_control.num_total_files == 0:
        logging.info(
            "Progress: {}%".format(
                thread_control.num_processed_files
                * 100.0
                / thread_control.num_total_files
            )
        )
    thread_control.mutexLock.release()


def index_object_manifest(
    commons_url,
    manifest_file,
    thread_num,
    auth=None,
    replace_urls=True,
    manifest_file_delimiter=None,
    output_filename="indexing-output-manifest.csv",
    submit_additional_metadata_columns=False,
    force_metadata_columns_even_if_empty=True,
):
    """
    Loop through all the files in the manifest, update/create records in indexd
    update indexd if the url is not in the record url list or acl has changed

    Args:
        commons_url(str): common url
        manifest_file(str): path to the manifest
        thread_num(int): number of threads for indexing
        auth(Gen3Auth): Gen3 auth or tuple with basic auth name and password
        replace_urls(bool): flag to indicate if replace urls or not
        manifest_file_delimiter(str): manifest's delimiter
        output_filename(str): output file name for manifest
        submit_additional_metadata_columns(bool): whether to submit additional metadata to the metadata service
        force_metadata_columns_even_if_empty(bool): force the creation of a metadata column
            entry for a GUID even if the value is empty. Enabling
            this will force the creation of metadata entries for every column.
            See below for an illustrative example

            Example manifest_file:
                guid, ..., columnA, columnB, ColumnC
                    1, ...,   dataA,        ,
                    2, ...,        ,   dataB,

            Resulting metadata if force_metadata_columns_even_if_empty=True :
                "1": {
                    "columnA": "dataA",
                    "columnB": "",
                    "ColumnC": "",
                },
                "2": {
                    "columnA": "",
                    "columnB": "dataB",
                    "ColumnC": "",
                },

            Resulting metadata if force_metadata_columns_even_if_empty=False :
                "1": {
                    "columnA": "dataA",
                },
                "2": {
                    "columnB": "dataB",
                },

    Returns:
        files(list(dict)): list of file info
        [
            {
                "guid": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        headers(list(str)): list of fieldnames

    """
    logging.info("Start the process ...")
    service_location = "index"
    commons_url = commons_url.strip("/")
    # if running locally, indexd is deployed by itself without a location relative
    # to the commons
    if "http://localhost" in commons_url:
        service_location = ""

    if not commons_url.endswith(service_location):
        commons_url += "/" + service_location

    logging.info("\nUsing URL {}\n".format(commons_url))

    indexclient = client.IndexClient(commons_url, "v0", auth=auth)

    if isinstance(auth, tuple):  # basic auth
        if submit_additional_metadata_columns:
            logging.warning(
                f"'submit_additional_metadata_columns' is on, but using indexd basic auth. Will not be able to submit to the metadata service. To create metadata, use Gen3Auth instance instead."
            )
        mds = None
    else:  # Gen3Auth
        mds = Gen3Metadata(auth_provider=auth)

    try:
        files, headers = get_and_verify_fileinfos_from_manifest(
            manifest_file, manifest_file_delimiter, include_additional_columns=True
        )
    except Exception as e:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        logging.error("Can not read {}. Detail: {}".format(manifest_file, e))
        return None, None

    # Early terminate
    if not files:
        return None, None

    try:
        headers.index(GUID_STANDARD_KEY)
    except ValueError:
        headers.insert(0, GUID_STANDARD_KEY)

    pool = ThreadPool(thread_num)

    thread_control = ThreadControl(num_total_files=len(files))
    part_func = partial(
        _index_record,
        indexclient,
        mds,
        replace_urls,
        thread_control,
        submit_additional_metadata_columns,
        force_metadata_columns_even_if_empty,
    )

    try:
        pool.map_async(part_func, files).get()
    except KeyboardInterrupt:
        pool.terminate()

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    output_filename = os.path.abspath(output_filename)
    logging.info(f"Writing output to {output_filename}")

    # remove existing output if it exists
    if os.path.isfile(output_filename):
        os.unlink(output_filename)

    _write_csv(os.path.join(CURRENT_DIR, output_filename), files, headers)

    return files, headers


@click.command()
@click.option(
    "--commons-url",
    "commons_url",
    help="Root domain (url) for a commons containing indexd.",
    required=True,
)
@click.option("--manifest_file", help="The path to input manifest")
@click.option(
    "--thread-num",
    "thread_num",
    type=int,
    help="Number of threads",
    default=1,
    show_default=True,
)
@click.option("--api-key", "api_key", help="path to api key")
@click.option("--auth", help="basic auth")
@click.option(
    "--replace-urls",
    "replace_urls",
    type=bool,
    help="If supplied, will replace urls for existing records. e.g. existing urls will be overwritten by the new ones",
    default=False,
    show_default=True,
)
@click.option(
    "--manifest-file-delimiter",
    "manifest_file_delimiter",
    help="string character that delimites the file (tab or comma). Defaults to tab.",
    default="\t",
    show_default=True,
)
@click.option(
    "--out-manifest-file",
    "out_manifest_file",
    help="The path to output manifest",
    default="indexing-output-manifest.csv",
    show_default=True,
)
def index_object_manifest_cli(
    commons_url,
    manifest_file,
    thread_num,
    api_key,
    auth,
    replace_urls,
    manifest_file_delimiter,
    out_manifest_file,
):
    """
    Commandline interface for indexing a given manifest to indexd

    Args:
        commons_url (str): root domain for common
        manifest_file (str): the full path to the manifest
        thread_num (int): number of threads being requested
        api_key (str): the path to api key
        auth(str): the basic auth
        replace_urls(bool): Replace urls or not
            NOTE: if both api_key and auth are specified, it will ignore the later and
            take the former as a default
        manifest_file_delimiter(str): manifest's delimiter
        out_manifest_file(str): path to the output manifest

    """

    if api_key:
        auth = Gen3Auth(commons_url, refresh_file=api_key)
    else:
        auth = tuple(auth.split(",")) if auth else None

    files, headers = index_object_manifest(
        commons_url + "/index",
        manifest_file,
        int(thread_num),
        auth,
        replace_urls,
        manifest_file_delimiter,
        output_filename=out_manifest_file,
    )


if __name__ == "__main__":
    logging.basicConfig(filename="index_object_manifest.log", level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    index_object_manifest_cli()


def delete_all_guids(auth, file):
    """
    Delete all GUIDs specified in the object manifest.

    WARNING: THIS COMPLETELY REMOVES INDEX RECORDS. USE THIS ONLY IF YOU KNOW
             THE IMPLICATIONS.
    """
    index = Gen3Index(auth.endpoint, auth_provider=auth)
    if not index.is_healthy():
        logging.debug(
            f"uh oh! The indexing service is not healthy in the commons {auth.endpoint}"
        )
        exit()

    # try to get delimeter based on file ext
    file_ext = os.path.splitext(file)
    if file_ext[-1].lower() == ".tsv":
        manifest_file_delimiter = "\t"
    else:
        # default, assume CSV
        manifest_file_delimiter = ","

    with open(file, "r", encoding="utf-8-sig") as input_file:
        csvReader = csv.DictReader(input_file, delimiter=manifest_file_delimiter)
        fieldnames = csvReader.fieldnames

        logging.debug(f"got fieldnames from {file}: {fieldnames}")

        # figure out which permutation of the name GUID is being used 1 time
        # then use it for all future rows
        guid_name = "guid"
        for name in ["guid", "GUID", "did", "DID"]:
            if name in fieldnames:
                guid_name = name

        logging.debug(f"using {guid_name} to retrieve GUID to delete...")

        for row in csvReader:
            guid = row.get(guid_name)

            if guid:
                logging.debug(f"deleting GUID record:{guid}")
                logging.debug(index.delete_record(guid=guid))

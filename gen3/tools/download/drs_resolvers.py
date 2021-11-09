from typing import List, Optional
import requests
import json
from cdiserrors import get_logger
import os
import inspect
from datetime import datetime, timezone, timedelta
from pathlib import Path

DRS_CACHE_EXPIRE_DURATION = os.getenv("DRS_CACHE_EXPIRE_DURATION", 2)  # In Days
DRS_CACHE_EXPIRE = timedelta(days=DRS_CACHE_EXPIRE_DURATION)
DRS_RESOLUTION_ORDER = os.getenv(
    "DRS_RESOLUTION_ORDER", "cache_file:commons_mds:dataguids_dist:dataguids"
)

DRS_CACHE = os.getenv(
    "DRS_CACHE", str(Path(Path.home(), ".drs_cache", "resolved_drs_hosts.json"))
)

logger = get_logger("download", log_level="warning")


def clean_dist_entry(s: str) -> str:
    """
    Cleans the string returning a proper DRS prefix
    @param s: string to clean
    @return: cleaned string
    """
    return s.replace("\\.", ".").replace(".*", "")


def clean_http_url(s: str) -> str:
    """
    Cleans input string removing http(s) prefix and all trailing paths
    @param s: string to clean
    @return: cleaned string
    """
    return (
        s.replace("/index", "")[::-1]
        .replace("/", "", 1)[::-1]
        .replace("http://", "")
        .replace("https://", "")
        .replace("/ga4gh/drs/v1/objects", "")
    )


def create_local_drs_cache(data: dict, cache_path: str = None) -> bool:
    if cache_path is None:
        cache_path = DRS_CACHE
    try:
        cache_path = Path(cache_path)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        # create timestam
        with open(cache_path, "wt") as fout:
            json.dump(
                {
                    "info": {
                        "created": datetime.now(timezone.utc).strftime(
                            "%m/%d/%Y %H:%M:%S:%z"
                        )
                    },
                    "cache": data,
                },
                fout,
            )
        return True

    except IOError as ex:
        logger.critical(f"cannot open {cache_path}: {ex}")
    return False


def append_to_local_drs_cache(data: dict, cache_path: str = None) -> bool:
    if cache_path is None:
        cache_path = DRS_CACHE
    try:
        cache_path = Path(cache_path)
        if cache_path.exists() is False:  # no cache exists so create a cache
            return create_local_drs_cache(data, cache_path)

        with open(cache_path, "rt") as fin:
            cache_data = json.load(fin)
            cache_data["cache"] = {**cache_data["cache"], **data}
            with open(cache_path, "wt") as fout:
                json.dump(cache_data, fout)

        return True

    except IOError as ex:
        logger.critical(f"cannot open {cache_path}: {ex}")
    except json.JSONDecodeError as ex:
        logger.error(f"json file cannot be parsed: {ex}")
    return False


def resolve_drs_from_local_cache(
    identifier: str, _: str = None, **kwargs
) -> Optional[str]:
    filename = kwargs.get("cache_dir", DRS_CACHE)
    if filename is None:
        return None
    # if no cache file, then return None, as this is not really an error
    if Path(DRS_CACHE).exists() is False:
        return None
    try:
        with open(filename, "rt") as fin:
            data = json.load(fin)
            if identifier in data["cache"]:
                timestamp = datetime.strptime(
                    data["cache"][identifier].get("created", "1/1/1900 00:00:01:+0000"),
                    "%m/%d/%Y %H:%M:%S:%z",
                )
                if (datetime.now(timezone.utc) - timestamp) > DRS_CACHE_EXPIRE:
                    fin.close()  # cache is expired return and one of the other resolvers will recreate it
                    return None
                return data["cache"][identifier].get("host", None)

    except json.JSONDecodeError as ex:
        logger.error(f"json cache file cannot be parsed: {ex}")

    return None


def resolve_compact_drs_using_indexd_dist(
    identifier: str,
    cache_results: bool = True,
    resolver_hostname: str = "https://dataguids.org",
):
    try:
        response = requests.get(f"{resolver_hostname}/index/_dist")
        response.raise_for_status()
        results = response.json()

        # convert to cached format
        data = {}
        for entry in results:
            if entry["type"] != "indexd":
                continue
            host = clean_http_url(entry["host"])
            name = entry.get("name", "")
            for x in entry["hints"]:
                id = clean_dist_entry(x)
                data[id] = {
                    "host": host,
                    "name": name,
                    "type": entry["type"],
                    "created": datetime.now(timezone.utc).strftime(
                        "%m/%d/%Y %H:%M:%S:%z"
                    ),
                }

        if identifier in data:
            if cache_results:  # write the results to cache since we have a
                # number of results, caching them will potentially
                # save lookup time in the future
                create_local_drs_cache(data)

            return data[identifier]["host"]

    except requests.exceptions.HTTPError as exc:
        logger.critical(
            f"HTTP Error accessing dataguids.org: {exc.response.status_code}"
        )
    return None


def resolve_drs_using_metadata_service(
    identifier: str, metadata_service_url: str, cache_results: bool = True
) -> Optional[str]:
    try:
        response = requests.get(f"{metadata_service_url}/{identifier}")
        response.raise_for_status()
        results = response.json()
        if "host" in results:
            hn = clean_http_url(results["host"])
            if cache_results:
                data = {
                    "identifier": {
                        "host": hn,
                        "name": results.get("name", ""),
                        "type": "indexd",
                        "created": datetime.now(timezone.utc).strftime(
                            "%m/%d/%Y %H:%M:%S:%z"
                        ),
                    }
                }
                append_to_local_drs_cache(data)
            return hn

    except requests.exceptions.HTTPError as exc:
        if exc.response.status_code != 404:
            logger.info(
                f"HTTP Error accessing dataguids.org: {exc.response.status_code}"
            )
        return None


def resolve_compact_drs_using_dataguids(
    identifier: str,
    object_id: str,
    cache_results: bool = True,
    resolver_hostname: str = "https://dataguids.org",
) -> Optional[str]:
    # use dataguids.org to resolve identifier
    # At this time there are two possible ways to resolve a compact ID
    # query https://dataguids.org/index/ with the object id and
    # look for the hostname in the field "from_index_service"
    # failing that try to access dataguids metadata service
    try:
        response = requests.get(f"{resolver_hostname}/index/{object_id}")
        response.raise_for_status()
        results = response.json()
        if (hn := results.get("from_index_service", {}).get("host", None)) is not None:
            hn = clean_http_url(hn)
            if cache_results:
                # create and entry to append to the cache
                data = {
                    "identifier": {
                        "host": hn,
                        "name": results.get("from_index_service", {}).get("name", None),
                        "type": "indexd",
                        "created": datetime.now(timezone.utc).strftime(
                            "%m/%d/%Y %H:%M:%S:%z"
                        ),
                    }
                }
                append_to_local_drs_cache(data)

            return hn

    except requests.exceptions.HTTPError as exc:
        if exc.response.status_code == 404:
            # not found try using the MDS
            pass
        else:
            return None

    return resolve_drs_using_metadata_service(
        identifier, f"{resolver_hostname}/mds/metadata", cache_results
    )


def resolve_drs_using_commons_mds(
    identifier: str, _: str, metadata_service_url: str, cache_results: bool = True
) -> Optional[str]:
    return resolve_drs_using_metadata_service(
        identifier, metadata_service_url, cache_results
    )


## TODO: provide methods to turn this into a plugin architecture
REGISTERED_DRS_RESOLVERS = {
    "cache_file": resolve_drs_from_local_cache,
    "commons_mds": resolve_drs_using_commons_mds,
    "dataguids_dist": resolve_compact_drs_using_indexd_dist,
    "dataguids": resolve_compact_drs_using_dataguids,
}


def resolve_drs_via_list(
    resolvers_to_try: List[str], identifier, object_id, **kwargs
) -> Optional[str]:
    tried = []
    for how in resolvers_to_try:
        tried.append(how)
        resolver = REGISTERED_DRS_RESOLVERS.get(how, None)
        if resolver is None:
            continue
        sig = inspect.signature(resolver)
        filter_keys = [
            param.name
            for param in sig.parameters.values()
            if param.kind == param.POSITIONAL_OR_KEYWORD
            and param.name not in ["identifier", "object_id", "_"]
        ]
        parameters_dict = {
            filter_key: kwargs[filter_key]
            for filter_key in filter_keys
            if filter_key in kwargs
        }

        host = resolver(identifier, object_id, **parameters_dict)
        if host is not None:
            logger.info(f"resolved {identifier} tried {tried}")
            return host

    logger.warning(f"unable to resolve {identifier} or {object_id}, tried {tried}")
    return None


def resolve_drs(identifier, object_id, **kwargs):
    return resolve_drs_via_list(
        DRS_RESOLUTION_ORDER.split(":"), identifier, object_id, **kwargs
    )

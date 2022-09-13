import csv
import json
import datetime
from cdislogging import get_logger
import tempfile
import asyncio
import os
import copy
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import date
import requests.exceptions

from gen3.metadata import Gen3Metadata
from gen3.submission import Gen3Submission
from gen3.tools import metadata
from gen3.utils import raise_for_status_and_print_error

MAX_GUIDS_PER_REQUEST = 2000
MAX_CONCURRENT_REQUESTS = 5
BASE_CSV_PARSER_SETTINGS = {
    "delimiter": "\t",
    "quotechar": "",
    "quoting": csv.QUOTE_NONE,
    "escapechar": "\\",
}

logging = get_logger("__name__")


def generate_discovery_metadata(auth, endpoint=None):
    """
    Get discovery metadata from dbgap for currently submitted studies in a commons
    """
    print(f"getting currently submitted project/study data from {endpoint}...")
    submission = Gen3Submission(endpoint, auth_provider=auth)
    query_txt = """
{
  project(first:0) {
    project_id
    code
    name
    studies(first:0) {
      study_id
      dbgap_phs
      dbgap_consent
      dbgap_version
      dbgap_accession
      dbgap_consent_text
      dbgap_participant_set
      authz
      full_name
      short_name
      study_description
      _subjects_count
    }
  }
}
    """
    raw_results = submission.query(query_txt).get("data", {}).get("project", [])
    results = []
    fields = set()

    print(f"parsing {endpoint} submission query...")
    for raw_result in raw_results:
        studies = raw_result.get("studies")
        study_data = {}
        if len(studies) != 1:
            logging.warning(
                f"expect 1:1 project:study, got {studies} from {raw_result}"
            )
        else:
            study_data = studies[0]

        del raw_result["studies"]
        result = copy.deepcopy(raw_result)
        result.update(study_data)

        if "authz" in result:
            result["authz"] = str(result["authz"]).replace("'", '"')

        result["tags"] = _determine_tags_from_study_info(result)
        result["study_description"] = _get_study_description(result)

        # don't include studies with no subjects for now, this effectively removes
        # any projects that were created but have no data submitted
        if result.get("_subjects_count"):
            results.append(result)

    fields = fields | set(result.keys())
    output_filepath = _dbgap_file_from_auth(auth)
    print(f"Writing to {output_filepath}...")
    with open(output_filepath, "w+", encoding="utf-8") as output_file:
        logging.info(f"writing headers to {output_filepath}: {fields}")
        output_writer = csv.DictWriter(
            output_file,
            delimiter="\t",
            fieldnames=fields,
            extrasaction="ignore",
        )
        output_writer.writeheader()

        for row in results:
            output_writer.writerow(row)


async def output_expanded_discovery_metadata(
    auth, endpoint=None, limit=500, use_agg_mds=False
):
    """
    fetch discovery metadata from a commons and output to {commons}-discovery-metadata.tsv
    """
    if endpoint:
        mds = Gen3Metadata(
            auth_provider=auth,
            endpoint=endpoint,
            service_location="mds/aggregate" if use_agg_mds else "mds",
        )
    else:
        mds = Gen3Metadata(
            auth_provider=auth,
            service_location="mds/aggregate" if use_agg_mds else "mds",
        )

    count = 0
    with tempfile.TemporaryDirectory() as metadata_cache_dir:
        all_fields = set()
        num_tags = 0

        for offset in range(0, limit, MAX_GUIDS_PER_REQUEST):
            partial_metadata = mds.query(
                "_guid_type=discovery_metadata",
                return_full_metadata=True,
                limit=min(limit, MAX_GUIDS_PER_REQUEST),
                offset=offset,
                use_agg_mds=use_agg_mds,
            )

            # if agg MDS we will flatten the results as they are in "common" : dict format
            # However this can result in duplicates as the aggregate mds is namespaced to
            # handle this, therefore prefix the commons in front of the guid
            if use_agg_mds:
                partial_metadata = {
                    f"{c}__{i}": d
                    for c, y in partial_metadata.items()
                    for x in y
                    for i, d in x.items()
                }

            if len(partial_metadata):
                for guid, guid_metadata in partial_metadata.items():
                    with open(
                        f"{metadata_cache_dir}/{guid.replace('/', '_')}", "w+"
                    ) as cached_guid_file:
                        guid_discovery_metadata = guid_metadata["gen3_discovery"]
                        json.dump(guid_discovery_metadata, cached_guid_file)
                        all_fields |= set(guid_discovery_metadata.keys())
                        num_tags = max(
                            num_tags, len(guid_discovery_metadata.get("tags", []))
                        )
            else:
                break

        output_columns = (
            ["guid"]
            # "tags" is flattened to _tag_0 through _tag_n
            + sorted(list(all_fields - set(["tags"])))
            + [f"_tag_{n}" for n in range(num_tags)]
        )
        base_schema = {column: "" for column in output_columns}

        output_filename = _metadata_file_from_auth(auth)
        with open(
            output_filename,
            "w+",
        ) as output_file:
            writer = csv.DictWriter(
                output_file,
                **{**BASE_CSV_PARSER_SETTINGS, "fieldnames": output_columns},
            )
            writer.writeheader()

            for guid in sorted(os.listdir(metadata_cache_dir)):
                with open(f"{metadata_cache_dir}/{guid}") as f:
                    fetched_metadata = json.load(f)
                    flattened_tags = {
                        f"_tag_{tag_num}": f"{tag['category']}: {tag['name']}"
                        for tag_num, tag in enumerate(fetched_metadata.pop("tags", []))
                    }

                    true_guid = guid
                    if use_agg_mds:
                        true_guid = guid.split("__")[1]
                    output_metadata = _sanitize_tsv_row(
                        {
                            **base_schema,
                            **fetched_metadata,
                            **flattened_tags,
                            "guid": true_guid,
                        }
                    )
                    writer.writerow(output_metadata)

        return output_filename


async def publish_discovery_metadata(
    auth,
    metadata_filename,
    endpoint=None,
    omit_empty_values=False,
    guid_type="discovery_metadata",
    guid_field=None,
    is_unregistered_metadata=False,
    reset_unregistered_metadata=False,
):
    """
    Publish discovery metadata from a tsv file
    """
    if endpoint:
        mds = Gen3Metadata(auth_provider=auth, endpoint=endpoint)
    else:
        mds = Gen3Metadata(auth_provider=auth)

    if not metadata_filename:
        metadata_filename = _metadata_file_from_auth(auth)

    delimiter = "," if metadata_filename.endswith(".csv") else "\t"

    with open(metadata_filename) as metadata_file:
        csv_parser_setting = {**BASE_CSV_PARSER_SETTINGS, "delimiter": delimiter}
        if is_unregistered_metadata:
            csv_parser_setting["quoting"] = csv.QUOTE_MINIMAL
            csv_parser_setting["quotechar"] = '"'
        metadata_reader = csv.DictReader(metadata_file, **{**csv_parser_setting})
        tag_columns = [
            column for column in metadata_reader.fieldnames if "_tag_" in column
        ]
        pending_requests = []

        if is_unregistered_metadata:
            registered_metadata_guids = mds.query(
                f"_guid_type={guid_type}", limit=2000, offset=0
            )
            guid_type = f"unregistered_{guid_type}"

        for metadata_line in metadata_reader:
            discovery_metadata = {
                key: _try_parse(value) for key, value in metadata_line.items()
            }

            if guid_field is None:
                guid = discovery_metadata.pop("guid")
            else:
                guid = discovery_metadata[guid_field]

            # when publishing unregistered metadata, skip those who are already registered if reset_unregistered_metadata is set to false
            if (
                not reset_unregistered_metadata
                and is_unregistered_metadata
                and str(guid) in registered_metadata_guids
            ):
                continue

            if len(tag_columns):
                # all columns _tag_0 -> _tag_n are pushed to a "tags" column
                coalesced_tags = [
                    {"name": tag_name.strip(), "category": tag_category.strip()}
                    for tag_category, tag_name in [
                        tag.split(":")
                        for tag in map(discovery_metadata.pop, tag_columns)
                        if tag != ""
                    ]
                ]
                discovery_metadata["tags"] = coalesced_tags

            if omit_empty_values:
                discovery_metadata = {
                    key: value
                    for key, value in discovery_metadata.items()
                    if value not in ["", [], {}]
                }

            metadata = {
                "_guid_type": guid_type,
                "gen3_discovery": discovery_metadata,
            }

            pending_requests += [mds.async_create(guid, metadata, overwrite=True)]
            if len(pending_requests) == MAX_CONCURRENT_REQUESTS:
                await asyncio.gather(*pending_requests)
                pending_requests = []

        await asyncio.gather(*pending_requests)


def try_delete_discovery_guid(auth, guid):
    """
    Deletes all discovery metadata under [guid] if it exists
    """
    mds = Gen3Metadata(auth_provider=auth)
    try:
        metadata = mds.get(guid)
        if metadata["_guid_type"] == "discovery_metadata":
            mds.delete(guid)
        else:
            logging.warning(f"{guid} is not discovery metadata. Skipping.")
    except requests.exceptions.HTTPError as e:
        logging.warning(e)


def _sanitize_tsv_row(tsv_row):
    sanitized = {}
    for k, v in tsv_row.items():
        if type(v) in [list, dict]:
            sanitized[k] = json.dumps(v)
        elif type(v) == str:
            sanitized[k] = v.replace("\n", "\\n")
    return sanitized


def _try_parse(data):
    if data:
        data = data.replace("\\n", "\n")
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data
    return ""


def _metadata_file_from_auth(auth):
    return (
        "-".join(urlparse(auth.endpoint).netloc.split(".")) + "-discovery_metadata.tsv"
    )


def _dbgap_file_from_auth(auth):
    return "-".join(urlparse(auth.endpoint).netloc.split(".")) + "-dbgap_metadata.tsv"


def _determine_tags_from_study_info(study):
    tags = []
    if study.get("project_id", "") and study.get("project_id", "").startswith("parent"):
        tags.append(_get_tag("Parent", "Program"))
        tags.append(_get_tag("DCC Harmonized", "Data Type"))
        tags.append(_get_tag("Clinical Phenotype", "Data Type"))

    if study.get("project_id", "") and study.get("project_id", "").startswith("topmed"):
        tags.append(_get_tag("TOPMed", "Program"))
        tags.append(_get_tag("Genotype", "Data Type"))

        if _is_topmed_study_geno_and_pheno(study.get("code", "")):
            tags.append(_get_tag("Clinical Phenotype", "Data Type"))

    if study.get("project_id", "") and study.get("project_id", "").startswith("COVID"):
        tags.append(_get_tag("COVID 19", "Program"))

    if study.get("dbgap_accession", "") and study.get("dbgap_accession", "").startswith(
        "phs"
    ):
        tags.append(_get_tag("dbGaP", "Study Registration"))

    return str(tags).replace("'", '"')


def _get_tag(name, category):
    return {"name": name, "category": category}


def _is_topmed_study_geno_and_pheno(study):
    # if the topmed study has both gennomic and phenotype data (instead of having a parent
    # study with pheno and a topmed with geno separately)
    #
    # determined from https://docs.google.com/spreadsheets/d/1iVOmZVu_IzsVMdefH-1Rgf8zrjqvnZOUEA2dxS5iRjc/edit#gid=698119570
    # filter to "program"=="topmed" and "parent_study_accession"==""
    return study in [
        "SAGE_DS-LD-IRB-COL",
        "Amish_HMB-IRB-MDS",
        "CRA_DS-ASTHMA-IRB-MDS-RD",
        "VAFAR_HMB-IRB",
        "PARTNERS_HMB",
        "WGHS_HMB",
        "BAGS_GRU-IRB",
        "Sarcoidosis_DS-SAR-IRB",
        "HyperGEN_GRU-IRB",
        "HyperGEN_DS-CVD-IRB-RD",
        "THRV_DS-CVD-IRB-COL-NPU-RD",
        "miRhythm_GRU",
        "AustralianFamilialAF_HMB-NPU-MDS",
        "pharmHU_HMB",
        "pharmHU_DS-SCD-RD",
        "pharmHU_DS-SCD",
        "SAPPHIRE_asthma_DS-ASTHMA-IRB-COL",
        "REDS-III_Brazil_SCD_GRU-IRB-PUB-NPU",
        "Walk_PHaSST_SCD_HMB-IRB-PUB-COL-NPU-MDS-GSO",
        "Walk_PHaSST_SCD_DS-SCD-IRB-PUB-COL-NPU-MDS-RD",
        "MLOF_HMB-PUB",
        "AFLMU_HMB-IRB-PUB-COL-NPU-MDS",
        "MPP_HMB-NPU-MDS",
        "INSPIRE_AF_DS-MULTIPLE_DISEASES-MDS",
        "DECAF_GRU",
        "GENAF_HMB-NPU",
        "JHU_AF_HMB-NPU-MDS",
        "ChildrensHS_GAP_GRU",
        "ChildrensHS_IGERA_GRU",
        "ChildrensHS_MetaAir_GRU",
        "CHIRAH_DS-ASTHMA-IRB-COL",
        "EGCUT_GRU",
        "IPF_DS-PUL-ILD-IRB-NPU",
        "IPF_DS-LD-IRB-NPU",
        "IPF_DS-PFIB-IRB-NPU",
        "IPF_HMB-IRB-NPU",
        "IPF_DS-ILD-IRB-NPU",
        "OMG_SCD_DS-SCD-IRB-PUB-COL-MDS-RD",
        "BioVU_AF_HMB-GSO",
        "LTRC_HMB-MDS",
        "PUSH_SCD_DS-SCD-IRB-PUB-COL",
        "GGAF_GRU",
        "PIMA_DS-ASTHMA-IRB-COL",
        "CARE_BADGER_DS-ASTHMA-IRB-COL",
        "CARE_TREXA_DS-ASTHMA-IRB-COL",
    ]


def _get_study_description(study):
    dbgap_phs = study.get("dbgap_phs", "") or ""
    dbgap_version = study.get("dbgap_version", "") or ""
    dbgap_participant_set = study.get("dbgap_participant_set", "") or ""
    dbgap_study = f"{dbgap_phs}.{dbgap_version}.{dbgap_participant_set}"
    print(f"Getting study description for {dbgap_study}...")

    study_description = study.get("study_description")
    if dbgap_study != "..":
        DBGAP_WEBSITE = (
            "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id="
        )
        url = DBGAP_WEBSITE + dbgap_study

        logging.debug(f"scraping {url}")
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "html.parser")

        report = soup.find("dl", class_="report")
        if report:
            study_description_start = report.find("dt")

            # sometimes the study description isn't the first "dd" tag
            if "Study Description" not in study_description_start.getText():
                study_description_start = study_description_start.find_next_sibling(
                    "dt"
                )

            study_description = study_description_start.find_next_sibling("dd") or ""

            if study_description:
                links = study_description.find(id="important-links")
                if links:
                    links.decompose()

                study_description = (
                    study_description.getText().strip().replace("\t", " ")
                    + f"\n\nNOTE: This text was scraped from https://www.ncbi.nlm.nih.gov/ on {date.today()} and may not include exact formatting or images."
                )
                logging.debug(f"{study_description}")

    return study_description

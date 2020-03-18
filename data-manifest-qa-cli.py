import argparse
import os
import logging
import sys

from gen3.tools import indexing
from gen3.tools.indexing.verify_manifest import manifest_row_parsers

# TODO: Maybe this script to its own repo to be distributed properly

# Debugging:
# $ export LOGLEVEL=DEBUG

# how to run:
# $ python data-manifest-qa-cli.py checkindex -m 1kG.tsv -e preprod.gen3.biodatacatalyst.nhlbi.nih.gov

LOGLEVEL = os.environ.get("LOGLEVEL", "WARNING").upper()
logging.basicConfig(level=LOGLEVEL, format="%(asctime)-15s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def _get_acl(row):
    try:
        acls = row.get("acl").strip().strip("[").strip("]").strip("'").split(",")
        sorted(acls)
        return acls
    except Exception:
        logging.warning(f"could not convert this to an int: {row.get('size')}")
        return row.get("size")


def _get_urls(row):
    acls = row.get("url").strip().split(" ")
    sorted(acls)
    return acls


def _get_file_size(row):
    try:
        return int(row.get("size"))
    except Exception:
        logging.warning(f"could not convert this to an int: {row.get('size')}")
    return row.get("size")


def make_parser():
    parser = argparse.ArgumentParser(
        description="QA'ing data release manifests",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""\
This script performs QA operations against Data Release manifests. It leverages the gen3sdk-python module to perform checks against a target Gen3 Commons environment and make sure the indexd-indexed records match the information in the manifest, among other formatting checks.
The general syntax for this script is:

data-manifest-qa-cli <command> <args>
e.g., data-manifest-qa-cli checkindex <manifest_file> <environment>

The most commonly used commands are:
   checkindex    Queries the Indexd records from a target environment to make sure the data matches what is in the manifest
             e.g. $ python data-manifest-qa-cli.py checkindex -m 1kG.tsv -e preprod.gen3.biodatacatalyst.nhlbi.nih.gov
""",
    )

    subparsers = parser.add_subparsers()

    parser_checkindex = subparsers.add_parser(
        "checkindex",
        description="Checks the indexd records to make sure we have matching data",
    )
    parser_checkindex.add_argument(
        "-m",
        "--manifest",
        dest="manifest",
        required=True,
        type=str,
        help="path to the manifest file (e.g., /Users/${USER}/Downloads/1kG.tsv)",
    )
    parser_checkindex.add_argument(
        "-e",
        "--env",
        dest="env",
        required=True,
        type=str,
        help="name of the environment (e.g., preprod.gen3.biodatacatalyst.nhlbi.nih.gov)",
    )

    parser.set_defaults(func=checkindex)
    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()
    if len(args._get_kwargs()) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    args.func(args)

    def _get_acl(row):
        try:
            acls = row.get("acl").strip().strip("[").strip("]").strip("'").split(",")
            sorted(acls)
            return acls
        except Exception:
            logging.warning(f"could not convert this to an int: {row.get('size')}")
            return row.get("size")

    def _get_urls(row):
        acls = row.get("url").strip().split(" ")
        sorted(acls)
        return acls

    def _get_file_size(row):
        try:
            return int(row.get("size"))
        except Exception:
            logging.warning(f"could not convert this to an int: {row.get('size')}")
            return row.get("size")


def checkindex(args):
    manifest_file = args.manifest
    target_env = args.env
    log.debug("manifest_file: {}".format(manifest_file))
    log.debug("target_env: {}".format(target_env))

    # override default parsers
    manifest_row_parsers["file_size"] = _get_file_size
    manifest_row_parsers["acl"] = _get_acl
    manifest_row_parsers["urls"] = _get_urls

    indexing.verify_object_manifest(
        "https://{}".format(target_env),
        manifest_file=manifest_file,
        num_processes=20
    )


if __name__ == "__main__":
    main()

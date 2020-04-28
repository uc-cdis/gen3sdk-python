"""
This script can be used to convert a file download manifest from https://dcc.icgc.org/
to a manifest that the gen3-client (https://github.com/uc-cdis/cdis-data-client) can use.

Examples:
python dcc_to_gen3.py -m dcc_manifest.sh
python dcc_to_gen3.py -m test_manifest_SGV.1587401968197.tar.gz

Arguments:
    - manifest(str, required): The manifest downloaded from the DCC portal,e.g., "/Users/christopher/Documents/Notes/ICGC/dcc_manifest.pdc.1581529402015.sh".
    - indexd(str, not required): If a file already exists with the data commons indexd records, provide the path to that file. Otherwise, if not provided, the indexd database will be queried until it collects all file records.

Use the generated manifest with the gen3-client, downloaded here: https://github.com/uc-cdis/cdis-data-client/releases/latest

Gen3-Client Example:
    gen3-client configure --profile=icgc --apiendpoint=https://icgc.bionimbus.org/ --cred=~/Downloads/icgc-credentials.json
    gen3-client download-multiple --profile=icgc --manifest=gen3_manifest_dcc_manifest.sh.json --no-prompt --download-path='icgc_pcawg_files'

"""
import json, requests, os, argparse, re, ast, time, tarfile
import pandas as pd

global args, guid_map, dcc_files, manifest_filename


class Gen3Error(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a gen3-client manifest from a DCC manifest using GUID / storage-location mappings.")
    parser.add_argument("-m", "--manifest", required=True, help="The manifest downloaded from DCC portal.",default="dcc_manifest.pdc.1581529402015.sh")
    parser.add_argument("-g", "--guid_map", required=False, help="File with PDC data file s3 location to GUID mapping.",default="PDC_s3_GUID_map_DCC_22199.txt")
    args = parser.parse_args()
    return args


def extract_dcc_manifest(dcc_file):
    # args.manifest = 'test_manifest_SGV.1587399656993.tar.gz'
    print("Reading dcc manifest file: '{}'".format(dcc_file))

    if dcc_file.endswith('tar.gz'):
        try:
            tar = tarfile.open(dcc_file)
            manifest_filename = tar.getnames()[0]
            tar.extractall()
            tar.close()
            print("Extracting tar.gz file '{}'. Manifest filename in archive '{}'".format(dcc_file,manifest_filename))
        except Exception as e:
            raise Gen3Error("Couldn't read the DCC manifest file '{}': {} \nPlease, try to unzip the manifest archive first and run script on extracted shell script (.sh file).".format(dcc_file,e))
    elif dcc_file.endswith('.sh'):
        manifest_filename = dcc_file
    else:
        raise Gen3Error("Please provide a manifest downloaded from dcc.icgc.org. The extension should be '.sh' or '.tar.gz'.\n'{}' is not a valid DCC manifest.".format(dcc_file))

    return manifest_filename

def read_dcc_manifest(manifest_filename):

    try:
        with open(manifest_filename, 'r') as mani:
            dcc_lines = [line.rstrip() for line in mani]
            dcc_regex = re.compile(r'^.+cp (s3.+) \.$')
            dcc_files = [dcc_regex.match(i).groups()[0] for i in dcc_lines if dcc_regex.match(i)]
            print("\t{} files found in the DCC manifest.".format(len(dcc_files)))
        return dcc_files

    except Exception as e:
        raise Gen3Error("Couldn't read the DCC manifest file '{}': {} ".format(dcc_file,e))


def read_dcc_mapping(map_file):
    """ read in the file with the mapping of s3 location to GUIDs for the 22,199 PDC files in dcc.icgc.org
    """
    mapping = {}
    try:
        print("Reading indexd GUID / storage location mappings: '{}'".format(map_file))
        with open(map_file, 'r') as mfile:
            mapping = mfile.readline()
            mapping = ast.literal_eval(mapping)
            print("\t{} file records were found in the indexd file.".format(len(mapping)))
    except Exception as e:
        raise Gen3Error("Couldn't load the file '{}': {} ".format(map_file,e))

    return mapping


def map_manifest(dcc_files,guid_map):
    """ make a map of files
    """
    mani_map = {k: v for (k,v) in guid_map.items() if k in dcc_files}
    print("Found {} matching files in GUID map from the DCC manifest ({} total files).".format(len(mani_map),len(dcc_files)))
    return mani_map


def write_manifest(mani_map):
    """ write the gen3-client manifest
    """

    count,total = (0,0)
    mname = "gen3_manifest_{}.json".format(manifest_filename)

    with open(mname, 'w') as manifest:

        manifest.write('[')
        for loc in mani_map:

            total += 1
            count += 1

            object_id = mani_map[loc]

            manifest.write('\n\t{')
            manifest.write('"object_id": "{}", '.format(object_id))
            manifest.write('"location": "{}"'.format(loc))

            if count == len(mani_map):
                manifest.write('  }]')
            else:
                manifest.write('  },')

            print("\tGUID: {}, location: {} ({}/{})".format(object_id,loc,count,total))

        print("\tDone ({}/{}). Guids for {} files out of the {} files listed in the DCC manifest written to gen3-client manifest:\n\t\t{}".format(count,total,len(mani_map),len(dcc_files),mname))

        if len(mani_map) != len(dcc_files):
            missing = list(set(dcc_files).difference(mani_map))
            print("Missing the following files:\n\t{}".format(missing))






if __name__ == "__main__":

    args = parse_args()

    try:
        manifest_filename = extract_dcc_manifest(dcc_file=args.manifest)
    except Exception as e:
        raise Gen3Error("\n\nCouldn't find provided DCC manifest or extract the archive: {} {}".format(type(e),e))

    try:
        dcc_files = read_dcc_manifest(manifest_filename=manifest_filename)
    except Exception as e:
        raise Gen3Error("\n\nCouldn't read the provided DCC manifest: {} {}".format(type(e),e))

    try:
        # guid_map = read_dcc_mapping(map_file=args.guid_map)
        mani_map = map_manifest(dcc_files=dcc_files,guid_map=guid_map)
    except Exception as e:
        raise Gen3Error("\n\nCouldn't load the provided GUID-location mapping: {} {}".format(type(e),e))

    try:
        write_manifest(mani_map=mani_map)
    except Exception as e:
        raise Gen3Error("\n\nCouldn't write the Gen3 manifest: {} {}".format(type(e),e))


else:
    # trouble-shooting
    api = "https://icgc.bionimbus.org/"
    args = lambda: None
    setattr(args, 'api', "https://icgc.bionimbus.org/")
    setattr(args, 'manifest', "dcc_manifest.sh")
    setattr(args, 'manifest', "test_manifest_SGV.1587401968197.tar.gz")
    setattr(args, 'limit', 1000)
    setattr(args, 'guids', "PDC_s3_GUID_map_DCC_22199.txt")
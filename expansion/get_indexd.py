"""
This script downloads all the indexd records for a data commons.

Examples:
python get_indexd.py -a https://icgc.bionimbus.org/

Arguments:
    - api(str, required): The manifest downloaded from the DCC portal,e.g., "/Users/christopher/Documents/Notes/ICGC/dcc_manifest.pdc.1581529402015.sh".

"""
import json, requests, os, argparse, re, time
import pandas as pd


global locs, irecs, args, token, all_records


def parse_args():
    parser = argparse.ArgumentParser(description="Retrieve indexd for a given data commons API.")
    parser.add_argument("-a", "--api", required=True, help="The data commons URL.",default="https://icgc.bionimbus.org/")
    parser.add_argument("-l", "--limit", type=int, required=False, help="The record limit per page use for paginating indexd records.",default=1000)
    args = parser.parse_args()
    return args


def get_indexd_page(limit,page=0):
    """ Queries indexd with given records limit and page number.
        For example:
            records = get_indexd_page(api='https://icgc.bionimbus.org/',limit=100,page=0)
            https://icgc.bionimbus.org/index/index/?limit=100&page=0
    """
    data,records = {},[]
    index_url = "{}/index/index/?limit={}&page={}".format(args.api,limit,page)

    try:
        response = requests.get(index_url).text
        data = json.loads(response)
    except Exception as e:
        print("\tUnable to parse indexd response as JSON!\n\t\t{} {}".format(type(e),e))

    if 'records' in data:
        records = data['records']

    elif 'error' in data:
        records = data

        if 'service failure' in data['error']:
            records = 'service failure'

    else:
        print("\tNo records found in data from '{}':\n\t\t{}".format(index_url,data))
        records = data

    return records


def get_indexd(limit,outfile=True):
    """ get all the records in indexd
        Usage:
            api = "https://icgc.bionimbus.org/"
            args = lambda: None
            setattr(args, 'api', "https://icgc.bionimbus.org/")
            setattr(args, 'limit', 1000)
    """

    if limit < 1 or limit > 1024:
        print("Indexd pagination limit must be between 1 and 1024: {}".format(limit))
        return

    stats_url = "{}/index/_stats".format(args.api)
    try:
        response = requests.get(stats_url).text
        stats = json.loads(response)
        filecount = stats['fileCount']
        print("Stats for '{}': {}".format(args.api,stats))
    except Exception as e:
        print("\tUnable to parse indexd response as JSON!\n\t\t{} {}".format(type(e),e))

    print("Getting all records in indexd (pagination limit: {})".format(limit))

    all_records,page,done = [],0,False
    while done is False:

        records = get_indexd_page(limit=limit,page=page)
        all_records.extend(records)

        if records == 'service failure':
            limit = int(limit/2)
            print("\tService failure! Reducing pagination limit to '{}'.".format(limit))
            time.sleep( 5 )
            if limit < 2:
                print("\n\nScript failed! Multiple service failures.")
                return all_records

        elif 'error' in records:
            print("\tError getting page '{}':\n\t{}".format(page,records))

        elif len(records) != limit:
            print("\tLength of returned records ({}) does not equal limit ({}).".format(len(records),limit))
            if len(records) == 0:
                done = True

        print("\tPage {}: {} records ({} total)".format(page,len(records),len(all_records)))
        page += 1

    print("\t\tScript finished. Total records retrieved: {}".format(len(all_records)))

    if outfile:
        dc_regex = re.compile(r'https:\/\/(.+)\/')
        dc = dc_regex.match(args.api).groups()[0]
        outname = "{}_indexd_records.txt".format(dc)
        with open(outname, 'w') as output:
            output.write(json.dumps(all_records))
        print("\t\t\tIndexd records saved to file: {}".format(outname))

    if len(all_records) != filecount:
        print("\n\nWarning: only {} of {} files in indexd downloaded!".format(filecount,len(all_records)))

    return all_records

# irecs = get_indexd(limit=1000)

def guid_location_mapping(irecs,outfile=True):
    """
    Returns GUID to file location mappings given a list of indexd records.
    Args:
        irecs(list): a list of indexd records returned from get_indexd() or query_indexd() functions.
    """
    locs = {irec['urls'][0]: irec['did'] for irec in irecs if len(irec['urls'])>=1}

    if outfile:
        dc_regex = re.compile(r'https:\/\/(.+)\/')
        dc = dc_regex.match(args.api).groups()[0]
        outname = "{}_GUID_location_mapping.txt".format(dc)
        with open(outname, 'w') as output:
            output.write("{}".format(locs))
        print("\t\t\tGUID/location mapping saved to file: {}".format(outname))

    return locs


if __name__ == "__main__":
    args = parse_args()
    irecs = get_indexd(limit=args.limit,outfile=True)
    mapping = guid_location_mapping(irecs,outfile=True)

else:
    # subset entire indexd / mapping to only 22,199 pdc_files in dcc.icgc.org:
    pdc_file = '/Users/christopher/Documents/Notes/ICGC/dcc_manifest.pdc.1581529402015.sh'
    pdc_files = read_dcc_manifest(pdc_file)

    precs = [irec for irec in irecs if len(irec['urls'])>0]
    precs = [irec for irec in precs if irec['urls'][0] in pdc_files]
    pdcmap = {k: v for (k,v) in mapping.items() if k in pdc_files}

    outrecs = "PDC_indexd_DCC_22199.txt".format(dc)
    with open(outrecs, 'w') as output:
        output.write("{}".format(precs))
    print("\t\t\tPDC GUID/location mapping saved to file: {}".format(outrecs))

    outmap = "PDC_s3_GUID_map_DCC_22199.txt".format(dc)
    with open(outmap, 'w') as output:
        output.write("{}".format(pdcmap))
    print("\t\t\tPDC GUID/location mapping saved to file: {}".format(outmap))

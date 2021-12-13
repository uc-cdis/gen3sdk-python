## Downloading DRS objects and manifest
TOC
- [JSON Object Manifest](#json-object-manifests)
- [Pulling Object Manifests](#pulling-object-manifests)
- [Listing Files and Access](#listing-files-and-access)
- [Resolving DRS Identifiers](#resolving-drs-identifiers)    

## JSON Object Manifests

A JSON manifest is a list of objects containing at a minimum a DRS 
object id:
```json
[
  {
    "object_id": "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"
  },
  {
    "object_id": "dg.XXTS/6f9a924f-9d83-4597-8f66-fe7d3021729f"
  }
]
```

Additional information (file_name, file_size, md5sum, and commons_url) are 
supported:

```json
[
  {
    "md5sum": "65196806d31002bd48abed020d861cf1",
    "file_name": "TestDataSet1.sav",
    "file_size": 1566369,
    "object_id": "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e",
    "commons_url": "test.commons1.io"
  },
  {
    "md5sum": "8371753a1324d421cc519cc03dcd477d",
    "file_name": "TestDataSet_April2020.sav",
    "file_size": 313525,
    "object_id": "dg.XXTS/6d3eb293-8388-4c5d-83ef-d0c2bd5ba604",
    "commons_url": "test.commons1.io"
  }
]
```

## Pulling Object Manifests 

To download using the CLI. Given a manifest JSON file. The cli can download the files in the 
manifest using:
```
gen3 cli pull_manifest <manifest.json> 
```
You should see something like:
```
EXAMPLE_TEST_DATAFILE1_Feb2020.sav    : 100%|████████████████████████████████████████████████| 1.57M/1.57M [00:00<00:00, 9.43MiB/s]
EXAMPLE_TEST_DATAFILE2_April2020.sav  : 100%|████████████████████████████████████████████████| 314k/314k [00:00<00:00, 2.86MiB/s]
EXAMPLE_TEST_DATAFILE3_June2020.sav   : 100%|████████████████████████████████████████████████| 370k/370k [00:00<00:00, 3.57MiB/s]
EXAMPLE_TEST_DATAFILE4_Oct2020.sav    : 100%|████████████████████████████████████████████████| 368k/368k [00:00<00:00, 3.30MiB/s]
```
Any error will be reported in the console. 

To download an individual object, the command can be of the form:
```
gen3 cli pull_object dg.XXTS/181af989-5d66-4139-91e7-69f4570ccd41
```
You should see something like:
```
Datafile05_T.csv    : 100%|████████████████████████████████████████████████████████████████████████| 3.72M/3.72M [00:01<00:00, 1.92MiB/s]
```

### Listing Files and Access

#### List Contents

To list the contents of a manifest file (or DRS object id):
```
gen3 cli ls test1_manifest.json
```
response:
```
                               Datafile01_T.csv      7.46 MB jcoin.datacommons.io 04/06/2021, 11:20:05
```
If the DRS object is a bundle, ls will expand the bundle and list it as a hierarchy:
```
gen3 cli --endpoint mycommons.datacommons.tw dir-pull ls --object dg.XXST/9bdc7951-e479-4ddc-9db1-15cf3307116b
```
the response:
```
ISPY1_series_1.3.6.1.4.1.14519.5.2.1.7695.1700.100392922143281268049004312967/1.3.6.1.4.1.14519.5.2.1.7695.1700.157333248288758912755025909591     23.22 MB nci-crdc.datacommons.io 12/04/2020, 18:55:40
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.339899736221920019384025688457.dcm#1592595549672921    527.66 KB nci-crdc.datacommons.io 09/20/2020, 00:29:37
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.118050407182881310601914734559.dcm#1592595549347460    527.66 KB nci-crdc.datacommons.io 09/18/2020, 16:52:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.663484117525683838268121809173.dcm#1592595549753443    527.66 KB nci-crdc.datacommons.io 09/18/2020, 22:17:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.307923524868494279703016890148.dcm#1592595549547255    527.66 KB nci-crdc.datacommons.io 09/19/2020, 00:11:53
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.177741488444741406265552879434.dcm#1592595549340161    527.66 KB nci-crdc.datacommons.io 09/19/2020, 09:12:09
    └── 1.3.6.1.4.1.14519.5.2.1.7695.1700.193538014659307853778370216921.dcm#1592595549438034    527.66 KB nci-crdc.datacommons.io 09/18/2020, 14:48:53
```
#### List Access
Given a manifest or DRS object id, you can query the access rights for that object's host commons, or in the case of a manifest all of the host commons

```
gen3 cli drs-pull ls --access test1_manifest.json
```

```
───────────────────────────────────────────────────────────────────────────────────────────────────────
Access for common1.org:
      /dictionary_page                                       :                                   access
      /programs/open                                         :                        read read-storage
      /programs/open/projects                                :                        read read-storage
      /programs/open/projects/Sample1                        :                        read read-storage
      /programs/open/projects/Test                           :                        read read-storage
      /sower                                                 :                                   access
      /workspace                                             :                                   access
───────────────────────────────────────────────────────────────────────────────────────────────────────
Access for data.othercommons.io:
      /data_file                                             :                              file_upload
      /mds_gateway                                           :                                   access
      /open                                                  :                        read read-storage
      /programs                                              :                                      * *
      /programs/test                                         :                                      * *
      /programs/test/projects                                :                                      * *
      /programs/test/projects/sample                         : * create delete read read-storage update write-storage *
      /services/indexd/admin                                 :                                        *
      /services/sheepdog/submission/program                  :                                        *
      /services/sheepdog/submission/project                  :                                        *
      /sower                                                 :                                   access
      /workspace                                             :                                   access

```

## Resolving DRS Identifiers

Downloading a DRS objectID or JSON manifest containing DRS bundles requires resolving the DRS prefix 
to an actual hostname. Most DRS resolvers request that the resolved prefixes are cached to prevent 
overloading these resolver services. Gen3 uses dataguid.org to resolve its prefixes but others
can be selected as well. 

To configure the DRS resolvers there are number of environment variables that can be set to control
the DRS resolution process.

* DRS_CACHE_EXPIRE_DURATION=2
* DRS_RESOLVER_HOSTNAME="https://dataguids.org"
* LOCALLY_CACHE_RESOLVED_HOSTS=True
* DRS_RESOLUTION_ORDER="cache_file:commons_mds:dataguids_dist:dataguids"

*DRS_CACHE_EXPIRE_DURATION* controls the number of days to keep a resolved DRS hostname, the default
value is 2 day but typically DRS hostnames do not change that often, so the value can be higher. 

*DRS_RESOLVER_HOSTNAME* set the hostname of the resolver service to use. Currently, the default 
value of "https://dataguids.org" is the only supported resolver, but others will be added in 
the future.

*LOCALLY_CACHE_RESOLVED_HOSTS* Set to True (the default) to create a local cache file to store the
resolved hostnames. Use of this is highly recommended as it will reduce the resolution time 
significantly. 

*DRS_RESOLUTION_ORDER* The order to apply the resolvers. The default value is the suggested one, and 
there should be no need to change this order. Note that the last one dataguids is needed as it is 
the final resolver when all others fail to resolve a DRS prefix. Depending on the configuration 
of the Gen3 commons the metadata service can also cache DRS prefixes but only when using the Aggregate
Metadata service.

Note that these environment variables allows for a Gen3 commons workspace to have a prepopulated 
cache file provided, which when combined with a large *DRS_CACHE_EXPIRE_DURATION* and a 
*DRS_RESOLUTION_ORDER* of "cache_file" only will prevent the DRS resolver from accessing any other 
resolver. This is recommended as one way to increase download performance. 

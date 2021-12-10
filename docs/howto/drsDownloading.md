## Downloading DRS objects and manifest
TOC
- [Resolving DRS Identifiers](#resolving-drs-identifiers)
- [Pulling Object Manifests](#pulling-object-manifests)
- [Listing Files and Access](#listing-files-and-access)
    

## Resolving DRS Identifiers

Downloading a DRS objectID or manifest containing DRS bundles requires resolving the DRS prefix 
to an actual hostname. Most DRS resolver request that the resolved prefixes are cached to prevent 
overloading these resolver services. Gen3 uses dataguid.org to resolve its prefixes but others
can be selected as well. 

To configure the DRS resolvers there are number of environment variables that can be set to control
the DRS resolution process.

* DRS_CACHE_EXPIRE_DURATION=2
* DRS_RESOLVER_HOSTNAME="http://dataguids.org"
* LOCALLY_CACHE_RESOLVED_HOSTS=True
* DRS_RESOLUTION_ORDER="cache_file:commons_mds:dataguids_dist:dataguids"

*DRS_CACHE_EXPIRE_DURATION* controls the number of days to keeps a resolved DRS hostname, the default
value is 2 day but typically DRS hostnames do not change that often, so the value can be higher. 

*DRS_RESOLVER_HOSTNAME* set the hostname of the resolver service to use. Currently, the default 
value of "http://dataguids.org" is the only supported resolver, but others will be added in 
the future.

*LOCALLY_CACHE_RESOLVED_HOSTS* Set to True (the default) to create a local cache file to store the
resolved hostnames. Use of this is highly recommended as it will reduce the resolution time 
significantly. 

*DRS_RESOLUTION_ORDER* The order to apply the resolvers. The default value is the suggested one, and 
there should be no need to change this order, not that the last one dataguids is needed as it is 
the final resolver when all others fail to resolve a DRS prefix. Depending on the configuration 
of the Gen3 commons the metadata service can also cache DRS prefixes but only when using the Aggregate
Metadata service.

Note that these environment variables allows for a Gen3 commons workspace to have a prepopulated 
cache file provided, which when combined with a large *DRS_CACHE_EXPIRE_DURATION* and a 
*DRS_RESOLUTION_ORDER* of "cache_file" only will prevent the DRS resolver from accessing any other 
resolver. This is recommended as one way to increase download performance. 

## Pulling Object Manifests 

To download using the CLI. Given a manifest JSON file. The cli can download the files in the 
manifest using:
```
gen3 cli pull_manifest <manifest.json> 
```
You should see something like:
```
JCOIN_NORC_Omnibus_SURVEY1_Feb2020.sav    : 100%|████████████████████████████████████████████████| 1.57M/1.57M [00:00<00:00, 9.43MiB/s]
JCOIN_NORC_Omnibus_SURVEY2_April2020.sav  : 100%|████████████████████████████████████████████████| 314k/314k [00:00<00:00, 2.86MiB/s]
JCOIN_NORC_Omnibus_SURVEY3_June2020.sav   : 100%|████████████████████████████████████████████████| 370k/370k [00:00<00:00, 3.57MiB/s]
JCOIN_NORC_Omnibus_SURVEY4_Oct2020.sav    : 100%|████████████████████████████████████████████████| 368k/368k [00:00<00:00, 3.30MiB/s]
```
Any error will be reported in the console. 

To download an individual object, the command can be of the form:
```
gen3 cli pull_object dg.6VTS/181af989-5d66-4139-91e7-69f4570ccd41
```
You should see something like:
```
Access05_T.csv    : 100%|████████████████████████████████████████████████████████████████████████| 3.72M/3.72M [00:01<00:00, 1.92MiB/s]
```

### Listing Files and Access

#### List Contents

To list the contents of a manifest file (or DRS object id):
```
gen3 cli ls test1_manifest.json
```
response:
```
                               Access01_T.csv      7.46 MB jcoin.datacommons.io 04/06/2021, 11:20:05
```
If the DRS object is a bundle, ls will expand the bundle and list it as a hierarchy:
```
gen3 cli --endpoint nci-crdc.datacommons.io ls --object dg.4DFC/9bdc7951-e479-4ddc-9db1-15cf3307116b
```
the response:
```
ISPY1_series_1.3.6.1.4.1.14519.5.2.1.7695.1700.100392922143281268049004312967/1.3.6.1.4.1.14519.5.2.1.7695.1700.157333248288758912755025909591     23.22 MB nci-crdc.datacommons.io 12/04/2020, 18:55:40
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.339899736221920019384025688457.dcm#1592595549672921    527.66 KB nci-crdc.datacommons.io 09/20/2020, 00:29:37
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.118050407182881310601914734559.dcm#1592595549347460    527.66 KB nci-crdc.datacommons.io 09/18/2020, 16:52:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.663484117525683838268121809173.dcm#1592595549753443    527.66 KB nci-crdc.datacommons.io 09/18/2020, 22:17:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.533895027716777172540373447016.dcm#1592595549640853    527.66 KB nci-crdc.datacommons.io 09/20/2020, 00:29:37
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.242120446485614851108115201256.dcm#1592595549439345    527.66 KB nci-crdc.datacommons.io 09/18/2020, 16:52:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.505003461105972384207075937898.dcm#1592595549634616    527.66 KB nci-crdc.datacommons.io 09/18/2020, 13:54:30
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.180470783025499613410439127104.dcm#1592595549330106    527.72 KB nci-crdc.datacommons.io 09/18/2020, 05:44:19
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.164122288434566150202471836088.dcm#1592595549343376    527.66 KB nci-crdc.datacommons.io 09/19/2020, 05:40:58
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.260329633264532085411944969074.dcm#1592595549544443    527.66 KB nci-crdc.datacommons.io 09/19/2020, 06:03:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.337570310267540047203015321424.dcm#1592595549632943    527.66 KB nci-crdc.datacommons.io 09/18/2020, 02:13:38
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.153579903918361623484276049852.dcm#1592595549333770    527.66 KB nci-crdc.datacommons.io 09/18/2020, 16:52:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.242636238347132086626263808659.dcm#1592595550042453    527.66 KB nci-crdc.datacommons.io 09/20/2020, 03:41:49
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.166146467125537753770428417531.dcm#1592595549355063    527.66 KB nci-crdc.datacommons.io 09/19/2020, 04:38:57
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.153311418918288633776755723728.dcm#1592595549329883    527.66 KB nci-crdc.datacommons.io 09/19/2020, 03:05:24
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.232015388224303460059469577237.dcm#1592595549479798    527.66 KB nci-crdc.datacommons.io 09/19/2020, 14:52:55
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.225396209544614722460808868261.dcm#1592595549442910    527.66 KB nci-crdc.datacommons.io 09/19/2020, 19:29:03
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.317075021581291120825035179079.dcm#1592595549644846    527.66 KB nci-crdc.datacommons.io 09/19/2020, 15:24:46
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.289353110965240705515988232471.dcm#1592595549555192    527.66 KB nci-crdc.datacommons.io 09/18/2020, 17:23:25
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.306741114257993813621749009156.dcm#1592595549541272    527.66 KB nci-crdc.datacommons.io 09/19/2020, 17:54:48
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.712397903588498199452789327736.dcm#1592595549738806    527.66 KB nci-crdc.datacommons.io 09/18/2020, 23:41:28
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.425406020479811181416664717377.dcm#1592595549631862    527.66 KB nci-crdc.datacommons.io 09/20/2020, 01:01:22
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.247792748811353847568869756226.dcm#1592595549559562    527.66 KB nci-crdc.datacommons.io 09/18/2020, 22:17:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.161742493475094818478958537880.dcm#1592595549438349    527.66 KB nci-crdc.datacommons.io 09/18/2020, 10:32:21
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.320720870689100768751705247895.dcm#1592595549636750    527.66 KB nci-crdc.datacommons.io 09/19/2020, 22:33:02
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.226218638118097567777635815790.dcm#1592595549455317    527.66 KB nci-crdc.datacommons.io 09/19/2020, 22:00:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.159425816006746458091241667673.dcm#1592595549332944    527.66 KB nci-crdc.datacommons.io 09/20/2020, 02:36:16
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.143209931345330033210442829273.dcm#1592595549559297    527.66 KB nci-crdc.datacommons.io 09/18/2020, 06:11:56
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.183256705851160295676286377683.dcm#1592595549439888    527.66 KB nci-crdc.datacommons.io 09/19/2020, 03:36:12
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.143682470730530721189306522317.dcm#1592595549377075    527.66 KB nci-crdc.datacommons.io 09/18/2020, 18:45:07
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.248915824616856740237278014287.dcm#1592595549539485    527.66 KB nci-crdc.datacommons.io 09/18/2020, 02:40:32
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.684087920592732384718693748439.dcm#1592595549732959    527.66 KB nci-crdc.datacommons.io 09/18/2020, 05:44:19
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.129454935840140560184348829034.dcm#1592595549338650    527.66 KB nci-crdc.datacommons.io 09/19/2020, 23:04:50
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.336659005443330634851457175572.dcm#1592595549648814    527.66 KB nci-crdc.datacommons.io 09/19/2020, 11:39:30
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.204452136097907104065521650758.dcm#1592595549451149    527.66 KB nci-crdc.datacommons.io 09/18/2020, 12:53:09
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.597840059188407136323514394597.dcm#1592595549886706    527.66 KB nci-crdc.datacommons.io 09/18/2020, 22:17:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.774041990962962078653285062025.dcm#1592595549729266    527.66 KB nci-crdc.datacommons.io 09/18/2020, 17:53:05
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.117522579202693935414953585449.dcm#1592595549243114    527.66 KB nci-crdc.datacommons.io 09/19/2020, 10:05:39
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.270083866510324669104754465806.dcm#1592595549531960    527.66 KB nci-crdc.datacommons.io 09/19/2020, 18:26:15
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.977826774591831109961975100349.dcm#1592595549735206    527.66 KB nci-crdc.datacommons.io 09/20/2020, 00:08:46
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.273139101477036335810245878802.dcm#1592595549536316    527.66 KB nci-crdc.datacommons.io 09/18/2020, 19:15:19
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.298638868786570826953774969758.dcm#1592595549468929    527.66 KB nci-crdc.datacommons.io 09/19/2020, 06:35:20
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.307923524868494279703016890148.dcm#1592595549547255    527.66 KB nci-crdc.datacommons.io 09/19/2020, 00:11:53
    ├── 1.3.6.1.4.1.14519.5.2.1.7695.1700.177741488444741406265552879434.dcm#1592595549340161    527.66 KB nci-crdc.datacommons.io 09/19/2020, 09:12:09
    └── 1.3.6.1.4.1.14519.5.2.1.7695.1700.193538014659307853778370216921.dcm#1592595549438034    527.66 KB nci-crdc.datacommons.io 09/18/2020, 14:48:53
```
#### List Access
Given a manifest or DRS object id, you can query the access rights for that object's host commons, or in the case of a manifest all of the host commons

```
gen3 cli ls --access test1_manifest.json
```

```
───────────────────────────────────────────────────────────────────────────────────────────────────────
Access for healdata.org:
      /dictionary_page                                       :                                   access
      /programs/open                                         :                        read read-storage
      /programs/open/projects                                :                        read read-storage
      /programs/open/projects/BACPAC                         :                        read read-storage
      /programs/open/projects/HOPE                           :                        read read-storage
      /programs/open/projects/Preventing_Opioid_Use_Disorder :                        read read-storage
      /sower                                                 :                                   access
      /workspace                                             :                                   access
───────────────────────────────────────────────────────────────────────────────────────────────────────
Access for jcoin.datacommons.io:
      /data_file                                             :                              file_upload
      /mds_gateway                                           :                                   access
      /open                                                  :                        read read-storage
      /programs                                              :                                      * *
      /programs/JCOIN                                        :                                      * *
      /programs/JCOIN/projects                               :                                      * *
      /programs/JCOIN/projects/BMC                           :                                      * *
      /programs/JCOIN/projects/Brown                         :                                      * *
      /programs/JCOIN/projects/Chestnut                      :                                      * *
      /programs/JCOIN/projects/FRI                           :                                      * *
      /programs/JCOIN/projects/IU                            :                                      * *
      /programs/JCOIN/projects/MAARC                         :                                      * *
      /programs/JCOIN/projects/NYSPI                         :                                      * *
      /programs/JCOIN/projects/NYU                           :                                      * *
      /programs/JCOIN/projects/OEPS                          :                    * read read-storage *
      /programs/JCOIN/projects/ROMI                          :                                      * *
      /programs/JCOIN/projects/SURVEYS                       :                                      * *
      /programs/JCOIN/projects/TCU                           :                                      * *
      /programs/JCOIN/projects/TEST                          :                                      * *
      /programs/JCOIN/projects/UKY                           :                    * read read-storage *
      /programs/JCOIN/projects/Yale                          :                                      * *
      /programs/test                                         :                                      * *
      /programs/test/projects                                :                                      * *
      /programs/test/projects/jcoin                          : * create delete read read-storage update write-storage *
      /prometheus                                            :                                   access
      /services/indexd/admin                                 :                                        *
      /services/sheepdog/submission/program                  :                                        *
      /services/sheepdog/submission/project                  :                                        *
      /sower                                                 :                                   access
      /workspace                                             :                                   access

```
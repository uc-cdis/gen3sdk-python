## Pulling Object manifests 

To download using the CLI. Given a manifest JSON file. The cli can download all
of the files in the manifest using:
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
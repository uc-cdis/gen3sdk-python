## FHIR 

This integration aims to enhance the data ingestion capabilities of Gen3 by integrating a Fast Healthcare Interoperability Resources (FHIR) data ingestion tools. FHIR is an important standard for working with Electronic Health Records (EHR) and we have started development of a Gen3 FHIR Proxy service. 

Gen3 is working on adding support for FHIR and these tools will help with data preparation.


The fhir commands can be invoked as follows

`gen3 fhir COMMAND [ARGS] [OPTIONS]`

For a list of commands and options run

`gen3 fhir --help`

For example, the following tags the 'Patient.ndjson' file with Gen3 authorization and outputs 'gen3_Patient.ndjson' using the authorization rules from 'config.yaml'. 

`gen3 fhir transform Patient.ndjson gen3_Patient.ndjson config.yaml --batch_size 10000`

INPUT_FILE:
There can only be one resource type per NDJSON file. 

OUTPUT_FILE:
Requires a distinct name/path from the input file to prevent overwriting the input file.

CONFIG.YAML:
The authorization configuration file has to be in yaml format and can have multiple conditions, as well as a `global_authz` which overrides all other rules. Conditions use [FHIRPath](https://hl7.org/fhirpath/) syntax, which is case-sensitive (e.g. `Specimen.type`, not `Specimen.Type`).

A resource matching two rules will throw an error and will require reconfiguration of the YAML file. 

Example rules:

```yaml
rules:
 - resource_type: "Patient"
   condition: "Patient.managingOrganization.reference = 'Organization/site-alpha'"
   authz: "/programs/Alpha/projects/Main"

#Example with multiple conditions
 - resource_type: "Specimen"
   condition: "Specimen.status = 'available' and Specimen.type.text = 'Blood specimen (specimen)'"
   authz: "/programs/Alpha/projects/Biobank"
```

An example config.yaml file can be found in [fhir_config.yaml](../../tests/test_data/fhir_config.yaml)
To run the fhir transform with synthetic test data:

`poetry run gen3 -vv fhir transform ./tests/test_data/Patient.ndjson ./test_Patient.ndjson ./tests/test_data/fhir_config.yaml`
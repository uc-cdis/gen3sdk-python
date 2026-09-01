## FHIR 

This integration aims to enhance the data ingestion capabilities of Gen3 by integrating a Fast Healthcare Interoperability Resources (FHIR) data ingestion pipeline & tools.  FHIR is an important standard for working with Electronic Health Records (EHR) and we have started development of a Gen3 FHIR Proxy service.  The overall goal is to allow users to seamlessly ingest data into an existing FHIR server. Gen3 is working on adding support for FHIR and this tool will help with data preparation and interaction in the future. 


The fhir commands can be invoked as follows

`gen3 fhir COMMAND [ARGS] [OPTIONS]`

For a list of commands and options run

`gen3 fhir --help`

For example, the following tags the 'Patient.ndjson' file with Gen3 authorization and outputs 'gen3_Patient.ndjson' using the authorization rules from 'config.yaml'

`gen3 fhir transform Patient.ndjson gen3_Patient.ndjson config.yaml --batch_size 10000 --workers 8 `


The authorization configuration file has to be in yaml format and can have multiple conditions, e.g:

```yaml
rules:
 - resource_type: "Patient"
   condition: "Patient.managingOrganization.reference = 'Organization/site-alpha'"
   authz: "/programs/Alpha/projects/Main"

#Example with multiple conditions
 - resource_type: "Specimen"
   condition: ""Specimen.status = 'available' and Specimen.Type = 'Blood specimen (specimen)'""
   authz: "/programs/Alpha/projects/Biobank"
```

And example config.yaml file can be found in [fhir_config.yaml](tests/test_data/fhir_config.yaml)

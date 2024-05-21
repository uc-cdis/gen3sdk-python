## Downloading files from external repositories

## External file metadata

The study metadata should indicate if data are hosted in an external repository.
This is specified in the `external_file_metadata` field. An example is shown below.


```json
{
  "_guid_type": "discovery_metadata",
  "gen3_discovery": {
    // Gen3 administrative fields
    ...,

        [
            {
            "external_oidc_idp": "qdr-keycloak",
            "file_retriever": "QDR",
            "study_id": "QDR_study_01"
            },
            {
            "external_oidc_idp": "qdr-keycloak",
            "file_retriever": "QDR",
            "file_id": "QDR_file_02"
            },
        ]
  }
}
```

The `external_oidc_idp` field is required. It is used to determine how to get a token from the `workspace token service`.

The `file_retriever` field is also required. It is used to determine how to retrieve the file.

The `study_id` and `file_id` fields are allowed but are not required.




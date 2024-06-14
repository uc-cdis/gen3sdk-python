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
            "external_oidc_idp": "externaldata-keycloak",
            "file_retriever": "QDR",
            "study_id": "QDR_study_01"
            },
            {
            "external_oidc_idp": "externaldata-keycloak",
            "file_retriever": "QDR",
            "file_id": "QDR_file_02"
            },
        ]
  }
}
```

The `'external_oidc_idp'` field is required. It is used to determine how to get a token from the `workspace token service`.

The `'file_retriever'` field is also required. It is used to determine how to retrieve the file.

The `'study_id'` and `'file_id'` fields are allowed but are not required.

## Example code with external file download using a retriever function

The code should import `download_files_from_metadata` as well as a retriever function.

Prior to running the download code, there should be a call to the WTS `authorization_url` endpoint,

`<WTS_HOSTNAME>/oauth2/authorization_url?idp=<EXTERNAL_OIDC_IDP>`

followed by the user logging in to the external-idp.

```python
from gen3.auth import Gen3Auth
from gen3.tools.download.external_file_download import download_files_from_metadata
# example retriever function
from heal.qdr_downloads import get_syracuse_qdr_files

# host for commons where wts_server is configured for QDR tokens
wts_hostname = "my-dev.planx-pla.net"
credentials_file = "credentials_my-dev.json"
# retriever will use Gen3Auth to request a QDR token from the gen3 commons
auth = Gen3Auth(refresh_file=credentials_file)
# the referenced retriever function should have been imported into this module
retrievers = {"QDR": get_syracuse_qdr_files}
download_path = "data/qdr"

test_external_file_metadata = [
    {
        "external_oidc_idp": "externaldata-keycloak",
        "file_retriever": "QDR",
        "study_id": "doi:10.5064/F6N2GOC9"
    }
]

download_status = download_files_from_metadata(
    hostname=wts_hostname,
    auth=auth,
    external_file_metadata=test_external_file_metadata,
    retrievers=retrievers,
    download_path=download_path
)
print(f"Download status = {download_status})
```

# Gen3 Cross-Commons Subject Linking and Crosswalk

A general solution that supports the linking of subjects across commons. A researcher should be able to “crosswalk” the information from one commons to combine with subject data from other commons.

We accomplish this by utilizing the Gen3 Framework Services API, specifically the metadata/semi-structured data support. The SDK simplifies the management of this crosswalk data within the framework services.

> Note that all data in the Gen3 Metadata API **MUST BE OPEN ACCESS AND CONTAIN NO PII**

## Centralized Mapping Information Format

Note that this block effectively represents subject-level data, with the crosswalk being a namespace within that overall block. This **example** is a metadata record from the Gen3 Metadata API (powered by the metadata-service).

```json
"GUID": {
  "crosswalk": {
    "subject": {
      "{{commons_url}}": {
        "{{field_name}}": {
          "value": "",
          "type": "",
          "description": ""
        }
        // ... more field entries here
      },
      // ... more commons entries here
      "mapping_methodologies": [
        ""
      ]
    }
  }
}

```

Example:

```json
"GUID": {
  "crosswalk": {
    "subject": {
      "https://gen3.biodatacatalyst.nhlbi.nih.gov": {
        "Subject.submitter_id": {
          "value": "phs002363.v1_RC-1358",
          "type": "gen3_node_property",
          "description": "These identifiers are constructed as part of the data ingestion process in BDCat and concatenate the study and version with the study-provided subject ID (with a _ delimiting)."
        }
      },
      "https://data.midrc.org": {
        "Case.submitter_id": {
          "value": "A01-00888",
          "type": "gen3_node_property",
          "description": "The uniquely assigned case identifier in MIDRC."
        },
        "Case.data_submission_guid": {
          "value": "foobar",
          "type": "gen3_node_property",
          "description": "The identifier for this subject as provided by the site’s submission of Datavant tokens to MIDRC."
        },
        "masked_n3c_id": {
          "value": "123dfj4ia5oi*@a",
          "type": "masked_n3c_id",
          "description": "Masked National COVID Consortium ID provided by a Linkage Honest Broker to the MIDRC system."
        }
      },
      "mapping_methodologies": [
        "NHLBI provided a file of subject IDs for the PETAL study that directly associate a PETAL ID with a BDCat Subject Identifier.",
        "A Linkage Honest Broker provided MIDRC with what Masked N3C IDs match MIDRC cases via a system-to-system handoff."
      ]
    }
  }
}
```

## Crosswalk Data Upload Using Gen3 SDK/CLI

### `crosswalk.csv`

To provide mapping from one commons identifier to another.

* Columns are pipe-delimited and contain necessary information for crosswalk metadata
  * `{{commons url}}|{{identifier type}}|{{identifier name}}`
  * `{{identifier type}}` for Gen3 Graph node property: `gen3_node_property`
  * `{{identifier name}}` for Gen3 Graph node property: `{{node}}.{{property}}`
* File name does not matter

### `crosswalk_optional_info.csv`

To provide descriptions for commons identifiers.

* `{{commons url}}, {{identifier name}}, {{description}}`
* File name does not matter

### Example 1

`crosswalk_1.csv`

```
https://data.midrc.org|gen3_node_property|Case.submitter_id, https://gen3.biodatacatalyst.nhlbi.nih.gov|gen3_node_property|Subject.submitter_id
A01-00888, phs002363.v1_RC-1358
…
```

(optional) `crosswalk_optional_info_1.csv`

> Note: MUST include headers `commons_url,identifier_name,description`

```
commons_url,identifier_name,description
https://data.midrc.org, Case.submitter_id, The uniquely assigned case identifier in MIDRC.
https://gen3.biodatacatalyst.nhlbi.nih.gov, Subject.submitter_id, These identifiers are constructed as part of the data ingestion process in BDCat and concatonate the study and version with the study-provided subject ID (with a _ delimiting).
```

Gen3 SDK Command

```
gen3 objects crosswalk publish ./tests/test_data/crosswalk/crosswalk_1.csv -m "NHLBI provided a file of subject IDs for the PETAL study that directly associate a PETAL ID with a BDCat Subject Identifier." --info ./tests/test_data/crosswalk/crosswalk_optional_info_1.csv
```

### Example 2

`crosswalk_2.csv`

```
https://data.midrc.org|gen3_node_property|Case.submitter_id, https://data.midrc.org|gen3_node_property|Case.data_submission_guid,
https://data.midrc.org|masked_n3c_id|Masked N3C ID
A01-00888, foobar, 123dfj4ia5oi*@a
…
```

(optional) `crosswalk_optional_info_2.csv`

> Note: MUST include headers `commons_url,identifier_name,description`

```
commons_url,identifier_name,description
https://data.midrc.org, Case.data_submission_guid, The identifier for this subject as provided by the site’s submission of Datavant tokens to MIDRC.
https://data.midrc.org,Masked N3C ID,Masked National COVID Consortium ID provided by a Linkage Honest Broker to the MIDRC system.
```

Gen3 SDK Command

```
gen3 objects crosswalk publish crosswalk_2.csv -m "A Linkage Honest Broker provided MIDRC with what Masked N3C IDs match MIDRC cases via a system-to-system handoff." --info crosswalk_optional_info_2.csv
```

## Gen3 SDK handling crosswalk.csv submission to MDS

```
gen3 objects crosswalk read/publish/verify/delete
```

- Parse provided `crosswalk.csv` file(s)
- Validate format (especially column names)
- Convert information from crosswalk.csv into a payload to push to the MDS based on "Centralized Mapping Information Format" above

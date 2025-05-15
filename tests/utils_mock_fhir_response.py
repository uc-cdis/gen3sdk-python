"""
Contains captured responses from NIH dbGaP's FHIR server
to be used in testing.
"""

MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000166 = {
    "resourceType": "ResearchStudy",
    "id": "phs000166",
    "meta": {
        "versionId": "1",
        "lastUpdated": "2022-02-14T01:54:41.865-05:00",
        "source": "#RmGqDkdNNL3uZunk",
        "security": [
            {
                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/DbGaPConcept-SecurityStudyConsent",
                "code": "public",
                "display": "public",
            }
        ],
    },
    "extension": [
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyOverviewUrl",
            "valueUrl": "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs000166.v2.p1",
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-ReleaseDate",
            "valueDate": "2009-09-09",
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents-StudyConsent",
                    "valueCoding": {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyConsents-StudyConsent",
                        "code": "phs000166.v2.p1 - 0",
                        "display": "NRUP",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents-StudyConsent",
                    "valueCoding": {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyConsents-StudyConsent",
                        "code": "phs000166.v2.p1 - 1",
                        "display": "ARR",
                    },
                },
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumPhenotypeDatasets",
                    "valueCount": {
                        "value": 9,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumMolecularDatasets",
                    "valueCount": {
                        "value": 1,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumVariables",
                    "valueCount": {
                        "value": 348,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumDocuments",
                    "valueCount": {
                        "value": 39,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumSubjects",
                    "valueCount": {
                        "value": 4046,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumSamples",
                    "valueCount": {
                        "value": 4046,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumSubStudies",
                    "valueCount": {"system": "http://unitsofmeasure.org", "code": "1"},
                },
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "Genome-Wide_Human_SNP_Array_6.0",
                            }
                        ]
                    },
                }
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "SNP Genotypes (Array)",
                            }
                        ]
                    },
                }
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Functional Variant in the Autophagy-Related 5 Gene Promotor is Associated with Childhood Asthma",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3335039",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Following the footprints of polymorphic inversions on SNP data: from detection to association tests",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4417146",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Association Study Identifies Novel Pharmacogenomic Loci For Therapeutic Response to Montelukast in Asthma",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4470685",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Measuring the Corticosteroid Responsiveness Endophenotype in Asthma",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4530065",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "African ancestry is associated with cluster-based childhood asthma subphenotypes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5984446",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The Pharmacogenetics and Pharmacogenomics of Asthma Therapy",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3298891",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Association Study of Short-Acting Ã\x9f2-Agonists. A Novel Genome-Wide Significant Locus on Chromosome 2 near ASB3",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4384768",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Differences in Candidate Gene Association between European Ancestry and African American Asthmatic Children",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3046166",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Identification of ATPAF1 as a novel candidate gene for asthma in children",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3185108",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide Association Identifies the T Gene as a Novel Asthma Pharmacogenetic Locus",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3381232",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genetic predictors associated with improvement of asthma symptoms in response to inhaled corticosteroids",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4112383",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Rank-based genome-wide analysis reveals the association of Ryanodine receptor-2 gene variants with childhood asthma among human populations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3708719",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A Common 16p11.2 Inversion Underlies the Joint Susceptibility to Asthma and Obesity",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3951940",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Epistasis between SPINK5 and TSLP Genes Contributes to Childhood Asthma",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4186896",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "FEATURE SCREENING FOR TIME-VARYING COEFFICIENT MODELS WITH ULTRAHIGH DIMENSIONAL LONGITUDINAL DATA",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5019497",
                        },
                    ],
                },
            ],
        },
    ],
    "identifier": [
        {
            "type": {
                "coding": [
                    {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/DbGaPConcept-DbGaPStudyIdentifier",
                        "code": "dbgap_study_id",
                        "display": "dbgap_study_id",
                    }
                ]
            },
            "value": "phs000166.v2.p1",
        }
    ],
    "title": "SNP Health Association Resource (SHARe) Asthma Resource Project (SHARP)",
    "status": "completed",
    "category": [
        {
            "coding": [
                {
                    "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyDesign",
                    "code": "Cross-Sectional",
                    "display": "Cross-Sectional",
                }
            ],
            "text": "Cross-Sectional",
        }
    ],
    "description": "\nSNP Health Association Resource (SHARe) Asthma Resource project (SHARP) is conducting a genome-wide analysis in adults and children who have participated in National Heart, Lung, and Blood Institute&#39;s clinical research trials on asthma. This includes 1041 children with asthma who participated in the Childhood Asthma Management Program (CAMP), 994 children who participated in one or five clinical trials conducted by the Childhood Asthma Research and Education (CARE) network, and 701 adults who participated in one of six clinical trials conducted by the Asthma Clinical Research Network (ACRN).\n\nThere are three study types. The longitudinal clinical trials can be subsetted for population-based and/or case-control analyses. Each of the childhood asthma studies has a majority of children participating as part of a parent-child trio. The ACRN (adult) studies are probands alone. Control genotypes will be provided for case-control analyses.\n",
    "enrollment": [{"reference": "Group/phs000166.v2.p1-all-subjects"}],
    "sponsor": {
        "reference": "Organization/NHLBI",
        "type": "Organization",
        "display": "National Heart, Lung, and Blood Institute",
    },
}


MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000007 = {
    "resourceType": "ResearchStudy",
    "id": "phs000007",
    "meta": {
        "versionId": "1",
        "lastUpdated": "2022-02-14T02:02:49.892-05:00",
        "source": "#kU6thnO0fhSOEQrq",
        "security": [
            {
                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/DbGaPConcept-SecurityStudyConsent",
                "code": "public",
                "display": "public",
            }
        ],
    },
    "extension": [
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyOverviewUrl",
            "valueUrl": "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs000007.v32.p13",
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-ReleaseDate",
            "valueDate": "2020-12-22",
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents-StudyConsent",
                    "valueCoding": {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyConsents-StudyConsent",
                        "code": "phs000007.v32.p13 - 0",
                        "display": "NRUP",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents-StudyConsent",
                    "valueCoding": {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyConsents-StudyConsent",
                        "code": "phs000007.v32.p13 - 1",
                        "display": "HMB-IRB-MDS",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyConsents-StudyConsent",
                    "valueCoding": {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyConsents-StudyConsent",
                        "code": "phs000007.v32.p13 - 2",
                        "display": "HMB-IRB-NPU-MDS",
                    },
                },
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumPhenotypeDatasets",
                    "valueCount": {
                        "value": 462,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumMolecularDatasets",
                    "valueCount": {
                        "value": 28,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumVariables",
                    "valueCount": {
                        "value": 80372,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumDocuments",
                    "valueCount": {
                        "value": 1493,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumAnalyses",
                    "valueCount": {
                        "value": 3029,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumSubjects",
                    "valueCount": {
                        "value": 15144,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumSamples",
                    "valueCount": {
                        "value": 51752,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Content-NumSubStudies",
                    "valueCount": {
                        "value": 8,
                        "system": "http://unitsofmeasure.org",
                        "code": "1",
                    },
                },
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "1000G_ref_panel",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "Affymetrix_100K",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "Affymetrix_50K",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "CVDSNP55v1_A",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "FHSOmni_Axiom_PLINK_Markerset",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "Genome-Wide_Human_SNP_Array_5.0",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "HRC1_ref_panel",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "HapMap_phaseII",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "HuEx-1_0-st",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "HumanExome-12v1_CHARGE-ExomeChip-v6",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "HumanMethylation450",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "HumanOmni5-4v1_B",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "LegacySet",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "SNP-PCR",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "WES_markerset_grc36",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "WES_markerset_grc37",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "WGS_markerset_grc36",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "WGS_markerset_grc37",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "custom_probe_set",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "exome_vcf_ESP6800",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "exome_vcf_grc37",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-StudyMarkersets-StudyMarkerset",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyMarkersets-StudyMarkerset",
                                "code": "target_markerset_grc36",
                            }
                        ]
                    },
                },
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "Methylation (CpG)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "miRNA Expression (Array)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "SNP Genotypes (NGS)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "SNP/CNV Genotypes (NGS)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "WGS",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "SNP Genotypes (imputed)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "mRNA Expression (Array)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "SNP Genotypes (PCR)",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "WXS",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "Legacy Genotypes",
                            }
                        ]
                    },
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-MolecularDataTypes-MolecularDataType",
                    "valueCodeableConcept": {
                        "coding": [
                            {
                                "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-MolecularDataTypes-MolecularDataType",
                                "code": "SNP Genotypes (Array)",
                            }
                        ]
                    },
                },
            ],
        },
        {
            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers",
            "extension": [
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Recent methods for polygenic analysis of genome-wide data implicate an important effect of common variants on cardiovascular disease risk",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3213201",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The SLC2A9 nonsynonymous Arg265His variant and gout: evidence for a population-specific effect on severity",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3218899",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Lack of association of Klotho gene variants with valvular and vascular calcification in Caucasians: a candidate gene study of the Framingham Offspring Cohort",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3224114",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Post-Genomic Update on a Classical Candidate Gene for Coronary Artery Disease: ESR1",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3260440",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Evaluating prognostic accuracy of biomarkers in nested caseâ\x80\x93control studies",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3276269",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Identification of Homogeneous Genetic Architecture of Multiple Genetically Correlated Traits by Block Clustering of Genome-Wide Associations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3312758",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Evaluating the Evidence for Transmission Distortion in Human Pedigrees",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3338262",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Knowledge-Driven Analysis Identifies a Geneâ\x80\x93Gene Interaction Affecting High-Density Lipoprotein Cholesterol Levels in Multi-Ethnic Populations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3359971",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Family-Based Association Studies for Next-Generation Sequencing",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3370281",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide linkage scan for plasma high density lipoprotein cholesterol, apolipoprotein A-1 and triglyceride variation among American Indian populations: the Strong Heart Family Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3388907",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Prediction of Expected Years of Life Using Whole-Genome Markers",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3405107",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Polygenic Effects of Common Single-Nucleotide Polymorphisms on Life Span: When Association Meets Causality",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3419841",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "How Genes Influence Life Span: The Biodemography of Human Survival",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3419845",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "d-Catenin Is Genetically and Biologically Associated with Cortical Cataract and Future Alzheimer-Related Structural and Functional Brain Changes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3439481",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide meta-analysis points to CTC1 and ZNF676 as genes regulating telomere homeostasis in humans",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3510758",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Copy Number Variation on Chromosome 10q26.3 for Obesity Identified by a Genome-Wide Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3537105",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Obesityâ\x80\x93insulin targeted genes in the 3p26-25 region in human studies and LG/J and SM/J mice",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3586585",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genomics of human health and aging",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3592948",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Inter-chromosomal level of genome organization and longevity-related phenotypes in humans",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3592956",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The role of lipid-related genes, aging-related processes, and environment in healthspan",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3602307",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A family-based joint test for mean and variance heterogeneity for quantitative traits",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4275359",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A Systematic Heritability Analysis of the Human Whole Blood Transcriptome",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4339826",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Assessment of Whole-Genome Regression for Type II Diabetes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4401705",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Association of Rare Loss-Of-Function Alleles in HAL, Serum Histidine Levels and Incident Coronary Heart Disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4406800",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The uniform-score gene set analysis for identifying common pathways associated with different diabetes traits",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4415316",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Triglyceride-Increasing Alleles Associated with Protection against Type-2 Diabetes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4447354",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Identification of a novel FGFRL1 MicroRNA target site polymorphism for bone mineral density in meta-analyses of genome-wide association studies",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4512621",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Birth Cohort, Age, and Sex Strongly Modulate Effects of Lipid Risk Alleles Identified in Genome-Wide Association Studies",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4546650",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Integrated allelic, transcriptional, and phenomic dissection of the cardiac effects of titin truncations in health and disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4560092",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genomic prediction of coronary heart disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5146693",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "KLB is associated with alcohol drinking, and its gene product Ã\x9f-Klotho is necessary for FGF21 regulation of alcohol preference",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5167198",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Uncoupling associations of risk alleles with endophenotypes and phenotypes: insights from the ApoB locus and heart-related traits",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5242299",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Using information of relatives in genomic prediction to apply effective stratified medicine",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5299615",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Metabolomic profiles as reliable biomarkers of dietary composition",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5320413",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Identification of IDUA and WNT16 Phosphorylation-Related Non-Synonymous Polymorphisms for Bone Mineral Density in Meta-Analyses of Genome-Wide Association Studies",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5362379",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Coordinated Action of Biological Processes during Embryogenesis Can Cause Genome-Wide Linkage Disequilibrium in the Human Genome and Influence Age-Related Phenotypes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5367637",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Structural variants caused by Alu insertions are associated with risks for many human diseases",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5441760",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "An Exploration of Gene-Gene Interactions and Their Effects on Hypertension",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5470022",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Age-associated microRNA expression in human peripheral blood is associated with all-cause mortality and age-related traits",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5770777",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Cytosolic proteome profiling of monocytes for male osteoporosis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5779619",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Large-scale Cognitive GWAS Meta-Analysis Reveals Tissue-Specific Neural Expression and Potential Nootropic Drug Targets",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5789458",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Network based subcellular proteomics in monocyte membrane revealed novel candidate genes involved in osteoporosis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5812280",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Penetrance of Polygenic Obesity Susceptibility Loci across the Body Mass Index Distribution",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5812888",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "rqt: an R package for gene-level meta-analysis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5860520",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Strong impact of natural-selectionâ\x80\x93free heterogeneity in genetics of age-related phenotypes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5892700",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Apolipoprotein E region molecular signatures of Alzheimer's disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6052488",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The Cortical Neuroimmune Regulator TANK Affects Emotional Processing and Enhances Alcohol Drinking: A Translational Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6430980",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Mendelian randomization evaluation of causal effects of fibrinogen on incident coronary heart disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6510421",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Pleiotropic Meta-Analysis of Age-Related Phenotypes Addressing Evolutionary Uncertainty in Their Molecular Mechanisms",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6524409",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genetic heterogeneity of Alzheimerâ\x80\x99s disease in subjects with and without hypertension",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6544706",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The Framingham Heart Study 100K SNP genome-wide association study resource: overview of 17 phenotype working group reports",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1995613",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A genome-wide association study for blood lipid phenotypes in the Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1995614",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Final Draft Status of the Epidemiology of Atrial Fibrillation",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2245891",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Association of Common Polymorphisms in GLUT9 Gene with Gout but Not with Coronary Artery Disease in a Large Case-Control Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2275796",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "SLEEP MEDICINE NEWS AND UPDATES",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2556905",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Bivariate Genome-Wide Linkage Analysis of Femoral Bone Traits and Leg Lean Mass: Framingham Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2659513",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Using Linkage Analysis to Identify Quantitative Trait Loci for Sleep Apnea in Relationship to Body Mass Index",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2677984",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "GENETIC MODEL FOR LONGITUDINAL STUDIES OF AGING, HEALTH, AND LONGEVITY AND ITS POTENTIAL APPLICATION TO INCOMPLETE DATA",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2691861",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A genome-wide association scan of RR and QT interval duration in three European genetically isolated populations. The EUROSPAN project",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2760953",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Epidemiology of Type 2 Diabetes and Cardiovascular Disease: Translation From Population to Prevention: The Kelly West Award Lecture 2009",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2909080",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Current Perceptions of the Epidemiology of Atrial Fibrillation",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2917063",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Consent for Genetic Research in the Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2923558",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Common variants in FLNB/CRTAP, not ARHGEF3 at 3p, are associated with osteoporosis in southern Chinese women",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2946578",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genetics of the Framingham Heart Study Population",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3014216",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Ethical and practical challenges of sharing data from genome-wide association studies: The eMERGE Consortium experience",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3129243",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide association of an integrated osteoporosis-related phenotype: is there evidence for pleiotropic genes?",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3290743",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Contributions of The Framingham Study to Stroke and Dementia Epidemiology at 60 years",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3380159",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Development and Application of a Longitudinal ECG Repository: the Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3483375",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Heritability of pulmonary function estimated from pedigree and whole-genome markers",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3766834",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Circulating Cell and Plasma microRNA Profiles Differ between Non-ST-Segment and ST-Segment-Elevation Myocardial Infarction",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3890357",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Effects of Long-Term Averaging of Quantitative Blood Pressure Traits on the Detection of Genetic Associations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4085637",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Phenotypic extremes in rare variant study designs",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4867440",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Cohort Profile: The Framingham Heart Study (FHS): overview of milestones in cardiovascular epidemiology",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5156338",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide association study of subclinical interstitial lung disease in MESA",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5437638",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Non-parametric genetic prediction of complex traits with latent Dirichlet process regression models",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5587666",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A whole-blood transcriptome meta-analysis identifies gene expression signatures of cigarette smoking",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5975607",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Protein Biomarkers of Cardiovascular Disease and Mortality in the Community",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6064847",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Framingham Heart Study 100K project: genome-wide associations for cardiovascular disease outcomes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1995607",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide association study of electrocardiographic and heart rate variability traits: the Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1995612",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Linkage and Association Scans for Quantitative Trait Loci of Serum Lactate Dehydrogenaseâ\x80\x94The Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2958689",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Evaluating the Evidence for Transmission Distortion in Human Pedigrees",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3338262",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Neuropeptide Y Gene Polymorphisms Confer Risk of Early-Onset Atherosclerosis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2602734",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Whole-genome association study identifies STK39 as a hypertension susceptibility gene",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2629209",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide Association and Follow-Up Replication Studies Identified ADAMTS18 and TGFBR3 as Bone Mass Candidate Genes in Different Ethnic Groups",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2667986",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide Association Analyses Suggested a Novel Mechanism for Smoking Behavior Regulated by IL15",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2700850",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Powerful Bivariate Genome-Wide Association Analyses Suggest the SOX6 Gene Influencing Both Obesity and Osteoporosis Phenotypes in Males",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2730014",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genetic Analysis of Variation in Human Meiotic Recombination",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2730532",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide significant predictors of metabolites in the one-carbon metabolism pathway",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2773275",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "New Lesions Detected by Single Nucleotide Polymorphism Arrayâ\x80\x93Based Chromosomal Analysis Have Important Clinical Impact in Acute Myeloid Leukemia",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2773477",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "ATRIUM: Testing Untyped SNPs in Case-Control Association Studies with Related Individuals",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2775837",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Association Study Identifies ALDH7A1 as a Novel Susceptibility Gene for Osteoporosis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2794362",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Association of three genetic loci with uric acid concentration and risk of gout: a genome-wide association study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2803340",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Pathway-based Genome-Wide Association Analysis Identified the Importance of EphrinA-EphR pathway for Femoral Neck Bone Geometry",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2818219",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "GWAF: an R package for genome-wide association analyses with family data",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2852219",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide association analysis of total cholesterol and high-density lipoprotein cholesterol levels using the Framingham Heart Study data",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2867786",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Natural selection in a contemporary human population",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2868295",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide association identifies OBFC1 as a locus involved in human leukocyte telomere biology",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2889047",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Runs of Homozygosity Identify a Recessive Locus 12q21.31 for Human Adult Height",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2913044",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A PCA-based method for ancestral informative markers selection in structured populations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2920624",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "HMGA2 Is Confirmed To Be Associated with Human Adult Height",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2972475",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Multi-locus Test Conditional on Confirmed Effects Leads to Increased Power in Genome-wide Association Studies",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2982824",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Joint influence of small-effect genetic variants on human longevity",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2984609",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Gene by Sex Interaction for Measures of Obesity in  the Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3021872",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A validation of the first genome-wide association study of calcaneus ultrasound parameters in the European Male Ageing Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3042372",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Meta-Analysis confirms CR1, CLU,                     and PICALM as Alzheimerâ\x80\x99s disease risk loci and reveals                     interactions with APOE genotypes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3048805",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Beyond Missing Heritability: Prediction of Complex Traits",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3084207",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Analyze Multivariate Phenotypes in Genetic Association Studies by Combining Univariate Association Tests",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3090041",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "SNP arrayâ\x80\x93based karyotyping: differences and similarities between aplastic anemia and hypocellular myelodysplastic syndromes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3128479",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Replication of Previous Genome-wide Association Studies of Bone Mineral Density in Premenopausal American Women",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3153352",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "IL21R and PTH May Underlie Variation of Femoral Neck Bone Mineral Density as Revealed by a Genome-wide Association Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3153368",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Association Study for Femoral Neck Bone Geometry",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3153387",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Pleiotropy of Osteoporosis-Related Phenotypes: The Framingham Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3153998",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Pathway-Based Genome-Wide Association Analysis Identified the Importance of Regulation-of-Autophagy Pathway for Ultradistal Radius BMD",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3153999",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Burden of Rare Sarcomere Gene Variants in the Framingham and Jackson Heart Study Cohorts",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3511985",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Atrial natriuretic peptide is negatively regulated by microRNA-425",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3726159",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "A Meta-analysis of Gene Expression Signatures of Blood Pressure and Hypertension",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4365001",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide Identification of microRNA Expression Quantitative Trait Loci",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4369777",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Subgroup specific incremental value of new markers for risk prediction",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3633735",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Gene Expression Signatures of Coronary Heart Disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3684247",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Evaluating incremental values from new predictors with net reclassification improvement in survival analysis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3686882",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Evaluating the Predictive Value of Biomarkers with Stratified Case-Cohort Design",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3718317",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Association between SNP heterozygosity and quantitative traits in the Framingham Heart Study",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3760672",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Gene-alcohol interactions identify several novel blood pressure loci including a promising locus near SLC16A9",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3860258",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Meta-analysis of genome-wide association data identifies novel susceptibility loci for obesity",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3888264",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Resampling Procedures for Making Inference under Nested Case-control Studies",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3891801",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Adopting nested caseâ\x80\x93control quota sampling designs for the evaluation of risk markers",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3903399",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Multistage genome-wide association meta-analyses identified two new loci for bone mineral density",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3943521",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide Analysis of Body Proportion Classifies Height-Associated Variants by Mechanism of Action and Implicates Genes Important for Skeletal Development",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4570286",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Estimation and Inference in Generalized Additive Coefficient Models for Nonlinear Interactions with High-Dimensional Covariates",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4578655",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide Survey of Runs of Homozygosity Identifies Recessive Loci for Bone Mineral Density in Caucasian and Chinese Populations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4615523",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Gene by Environment Interaction Analysis Identifies Common SNPs at 17q21.2 that Are Associated with Increased Body Mass Index Only among Asthmatics",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4684413",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The Toll-Like Receptor 4 (TLR4) Variant rs2149356 and Risk of Gout in European and Polynesian Sample Sets",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4726773",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Exploring the major sources and extent of heterogeneity in a genome-wide association meta-analysis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4761279",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "FTO association and interaction with time spent sitting",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4783205",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Gene by stress genome-wide interaction analysis and path analysis identify EBF1 as a cardiovascular and metabolic risk gene",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4795045",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Understanding multicellular function and disease with human tissue-specific networks",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4828725",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Interaction of Insulin Resistance and Related Genetic Variants with Triglyceride-Associated Genetic Variants",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4838530",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Body mass index change in gastrointestinal cancer and chronic obstructive pulmonary disease is associated with Dedicator of Cytokinesis 1",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5476850",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "GWAS meta-analysis reveals novel loci and genetic correlates for general cognitive function: a report from the COGENT consortium",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5659072",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Identification of polymorphisms in cancer patients that differentially affect survival with age",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5680559",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Estimation of genomic prediction accuracy from reference populations with varying degrees of relationship",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5739427",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Integrative network analysis reveals molecular mechanisms of blood pressure regulation",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4422556",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Metabolomic Profiles of Body Mass Index in the Framingham Heart Study Reveal Distinct Cardiometabolic Phenotypes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4749349",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Diverse human extracellular RNAs are widely detected in human plasma",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4853467",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "An exome array study of the plasma metabolome",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4962516",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Messenger RNA and MicroRNA transcriptomic signatures of cardiometabolic risk factors",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5299677",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Micro RNAs from DNA Viruses are Found Widely in Plasma in a Large Observational Human Population",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5913337",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genetic architecture of gene expression traits across diverse populations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6105030",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Pathway-based analysis of genome-wide association study of circadian phenotypes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6163116",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Meta-Analysis across Cohorts for Heart and Aging Research in Genomic Epidemiology (CHARGE) Consortium Provides Evidence for an Association of Serum Vitamin D with Pulmonary Function",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6263170",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Assessing the genetic correlations between early growth parameters and bone mineral density: A polygenic risk score analysis",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6298225",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-wide analysis of genetic predisposition to Alzheimerâ\x80\x99s disease and related sex disparities",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6330399",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Independent associations of TOMM40 and APOE variants with body mass index",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6351823",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Replication of 6 Obesity Genes in a Meta-Analysis of Genome-Wide Association Studies from Diverse Ancestries",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4039436",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genetic analysis of candidate SNPs for metabolic syndrome in obstructive sleep apnea (OSA)",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4039742",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Assessing the phenotypic effects in the general population of rare variants in genes for a dominant mendelian form of diabetes",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4051627",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "An Examination of the Relationship between Hotspots and Recombination Associated with Chromosome 21 Nondisjunction",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4057233",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Gene-gene interaction between RBMS3 and ZNF516 influences bone mineral density",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4127986",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "The power comparison of the haplotype-based collapsing tests and the variant-based collapsing tests for detecting rare variants in pedigrees",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4131059",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Multiple Metabolic Genetic Risk Scores and Type 2 Diabetes Risk in Three Racial/Ethnic Groups",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4154088",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Abundant local interactions in the 4p16.1 region suggest functional mechanisms underlying SLC2A9 associations with human serum uric acid",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4159153",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Genome-Wide Meta-Analysis of Myopia and Hyperopia Provides Evidence for Replication of 11 Loci",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4169415",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Gene-Based Rare Allele Analysis Identified a Risk Gene of Alzheimerâ\x80\x99s Disease",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4203677",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Integrative analysis of multiple diverse omics datasets by sparse group multitask regression",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4209817",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Integrative Analysis of GWASs, Human Protein Interaction, and Gene Expression Identified Gene Modules Associated With BMDs",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4223444",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Meta-analysis of genome-wide association studies for circulating phylloquinone concentrations",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4232014",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Determinants of Power in Gene-Based Burden Testing for Monogenic Disorders",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5011058",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Non-linear interactions between candidate genes of myocardial infarction revealed in mRNA expression profiles",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5027110",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Pleiotropic Meta-Analyses of Longitudinal Studies Discover Novel Genetic Variants Associated with Age-Related Diseases",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5061751",
                        },
                    ],
                },
                {
                    "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer",
                    "extension": [
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Title",
                            "valueString": "Pleiotropic Associations of Allelic Variants in a 2q22 Region with Risks of Major Human Diseases and Mortality",
                        },
                        {
                            "url": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/StructureDefinition/ResearchStudy-Citers-Citer-Url",
                            "valueUrl": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5104356",
                        },
                    ],
                },
            ],
        },
    ],
    "identifier": [
        {
            "type": {
                "coding": [
                    {
                        "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/DbGaPConcept-DbGaPStudyIdentifier",
                        "code": "dbgap_study_id",
                        "display": "dbgap_study_id",
                    }
                ]
            },
            "value": "phs000007.v32.p13",
        }
    ],
    "title": "Framingham Cohort",
    "status": "completed",
    "category": [
        {
            "coding": [
                {
                    "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/CodeSystem/ResearchStudy-StudyDesign",
                    "code": "Prospective Longitudinal Cohort",
                    "display": "Prospective Longitudinal Cohort",
                }
            ],
            "text": "Prospective Longitudinal Cohort",
        }
    ],
    "focus": [
        {
            "coding": [
                {
                    "system": "urn:oid:2.16.840.1.113883.6.177",
                    "code": "D002318",
                    "display": "Cardiovascular Diseases",
                }
            ],
            "text": "Cardiovascular Diseases",
        }
    ],
    "condition": [
        {
            "coding": [
                {
                    "system": "urn:oid:2.16.840.1.113883.6.177",
                    "code": "D002318",
                    "display": "Cardiovascular Diseases",
                }
            ],
            "text": "Cardiovascular Diseases",
        },
        {
            "coding": [
                {
                    "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/NamingSystem/MeshEntryTerm",
                    "code": "D002318 entry term: Disease, Cardiovascular",
                    "display": "Disease, Cardiovascular",
                }
            ],
            "text": "Disease, Cardiovascular",
        },
        {
            "coding": [
                {
                    "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/NamingSystem/MeshEntryTerm",
                    "code": "D002318 entry term: Diseases, Cardiovascular",
                    "display": "Diseases, Cardiovascular",
                }
            ],
            "text": "Diseases, Cardiovascular",
        },
        {
            "coding": [
                {
                    "system": "https://uts.nlm.nih.gov/metathesaurus.html",
                    "code": "C0007222",
                    "display": "Cardiovascular Disease",
                }
            ],
            "text": "Cardiovascular Disease",
        },
    ],
    "keyword": [
        {
            "coding": [
                {
                    "system": "urn:oid:2.16.840.1.113883.6.177",
                    "code": "D002318",
                    "display": "Cardiovascular Diseases",
                }
            ],
            "text": "Cardiovascular Diseases",
        },
        {
            "coding": [
                {
                    "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/NamingSystem/MeshEntryTerm",
                    "code": "D002318 entry term: Disease, Cardiovascular",
                    "display": "Disease, Cardiovascular",
                }
            ],
            "text": "Disease, Cardiovascular",
        },
        {
            "coding": [
                {
                    "system": "https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1/NamingSystem/MeshEntryTerm",
                    "code": "D002318 entry term: Diseases, Cardiovascular",
                    "display": "Diseases, Cardiovascular",
                }
            ],
            "text": "Diseases, Cardiovascular",
        },
        {
            "coding": [
                {
                    "system": "https://uts.nlm.nih.gov/metathesaurus.html",
                    "code": "C0007222",
                    "display": "Cardiovascular Disease",
                }
            ],
            "text": "Cardiovascular Disease",
        },
    ],
    "description": '\n\t**Startup of Framingham Heart Study.** Cardiovascular disease (CVD) is the leading cause of death and serious illness in the United States. In 1948, the Framingham Heart Study (FHS) -- under the direction of the National Heart Institute (now known as the National Heart, Lung, and Blood Institute, NHLBI) -- embarked on a novel and ambitious project in health research. At the time, little was known about the general causes of heart disease and stroke, but the death rates for CVD had been increasing steadily since the beginning of the century and had become an American epidemic.\n\nThe objective of the FHS was to identify the common factors or characteristics that contribute to CVD by following its development over a long period of time in a large group of participants who had not yet developed overt symptoms of CVD or suffered a heart attack or stroke.\n\n**Design of Framingham Heart Study.** In 1948, the researchers recruited 5,209 men and women between the ages of 30 and 62 from the town of Framingham, Massachusetts, and began the first round of extensive physical examinations and lifestyle interviews that they would later analyze for common patterns related to CVD development. Since 1948, the subjects have returned to the study every two years for an examination consisting of a detailed medical history, physical examination, and laboratory tests, and in 1971, the study enrolled a second-generation cohort -- 5,124 of the original participants&#39; adult children and their spouses -- to participate in similar examinations. The second examination of the Offspring cohort occurred eight years after the first examination, and subsequent examinations have occurred approximately every four years thereafter. In April 2002 the Study entered a new phase: the enrollment of a third generation of participants, the grandchildren of the original cohort. The first examination of the Third Generation Study was completed in July 2005 and involved 4,095 participants. Thus, the FHS has evolved into a prospective, community-based, three generation family study. The FHS is a joint project of the National Heart, Lung and Blood Institute and Boston University.\n\n**Research Areas in the Framingham Heart Study.** Over the years, careful monitoring of the FHS population has led to the identification of the major CVD risk factors -- high blood pressure, high blood cholesterol, smoking, obesity, diabetes, and physical inactivity -- as well as a great deal of valuable information on the effects of related factors such as blood triglyceride and HDL cholesterol levels, age, gender, and psychosocial issues. Risk factors have been identified for the major components of CVD, including coronary heart disease, stroke, intermittent claudication, and heart failure. It is also clear from research in the FHS and other studies that substantial subclinical vascular disease occurs in the blood vessels, heart and brain that precedes clinical CVD. With recent advances in technology, the FHS has enhanced its research capabilities and capitalized on its inherent resources by the conduct of high resolution imaging to detect and quantify subclinical vascular disease in the major blood vessels, heart and brain. These studies have included ultrasound studies of the heart (echocardiography) and carotid arteries, computed tomography studies of the heart and aorta, and magnetic resonance imaging studies of the brain, heart, and aorta. Although the Framingham cohort is primarily white, the importance of the major CVD risk factors identified in this group have been shown in other studies to apply almost universally among racial and ethnic groups, even though the patterns of distribution may vary from group to group. In the past half century, the Study has produced approximately 1,200 articles in leading medical journals. The concept of CVD risk factors has become an integral part of the modern medical curriculum and has led to the development of effective treatment and preventive strategies in clinical practice.\n\nIn addition to research studies focused on risk factors, subclinical CVD and clinically apparent CVD, Framingham investigators have also collaborated with leading researchers from around the country and throughout the world on projects involving some of the major chronic illnesses in men and women, including dementia, osteoporosis and arthritis, nutritional deficiencies, eye diseases, hearing disorders, and chronic obstructive lung diseases.\n\n**Genetic Research in the Framingham Heart Study.** While pursuing the Study&#39;s established research goals, the NHLBI and the Framingham investigators has expanded its research mission into the study of genetic factors underlying CVD and other disorders. Over the past two decades, DNA has been collected from blood samples and from immortalized cell lines obtained from Original Cohort participants, members of the Offspring Cohort and the Third Generation Cohort. Several large-scale genotyping projects have been conducted in the past decade. Genome-wide linkage analysis has been conducted using genotypes of approximately 400 microsatellite markers that have been completed in over 9,300 subjects in all three generations. Analyses using microsatellite markers completed in the original cohort and offspring cohorts have resulted in over 100 publications, including many publications from the Genetics Analysis Workshop 13. Several other recent collaborative projects have completed thousands of SNP genotypes for candidate gene regions in subsets of FHS subjects with available DNA. These projects include the Cardiogenomics Program of the NHLBI&#39;s Programs for Genomics Applications, the genotyping of ~3000 SNPs in inflammation genes, and the completion of a genome-wide scan of 100,000 SNPs using the Affymetrix 100K Genechip.\n\n**Framingham Cohort Phenotype Data.** The phenotype database contains a vast array of phenotype information available in all three generations. These will include the quantitative measures of the major risk factors such as systolic blood pressure, total and HDL cholesterol, fasting glucose, and cigarette use, as well as anthropomorphic measures such as body mass index, biomarkers such as fibrinogen and CRP, and electrocardiography measures such as the QT interval. Many of these measures have been collected repeatedly in the original and offspring cohorts. Also included in the SHARe database will be an array of recently collected biomarkers, subclinical disease imaging measures, clinical CVD outcomes as well as an array of ancillary studies. The phenotype data is located here in the top-level study phs000007 Framingham Cohort. To view the phenotype variables collected from the Framingham Cohort, please click on the "Variables" tab above. \n\n**The Framingham Cohort is utilized in the following dbGaP substudies.** To view genotypes, analysis, expression data, other molecular data, and derived variables collected in these substudies, please click on the following substudies below or in the "Substudies" section of this top-level study page phs000007 Framingham Cohort.  - [phs000342](./study.cgi?study_id=phs000342) Framingham SHARe - [phs000282](./study.cgi?study_id=phs000282) Framingham CARe - [phs000363](./study.cgi?study_id=phs000363) Framingham SABRe - [phs000307](./study.cgi?study_id=phs000307) Framingham Medical Resequencing - [phs000401](./study.cgi?study_id=phs000401) Framingham ESP Heart-GO - [phs000651](./study.cgi?study_id=phs000651) Framingham CHARGE-S - [phs000724](./study.cgi?study_id=phs000724) Framingham DNA Methylation - [phs001610](./study.cgi?study_id=phs001610) Framingham T2D-GENES  \n\nThe unflagging commitment of the research participants in the NHLBI FHS has made more than a half century of research success possible. For decades, the FHS has made its data and DNA widely available to qualified investigators throughout the world through the Limited Access Datasets and the FHS DNA Committee, and the SHARe database will continue that tradition by allowing access to qualified investigators who agree to the requirements of data access. With the SHARe database, we continue with an ambitious research agenda and look forward to new discoveries in the decades to come.\n',
    "enrollment": [{"reference": "Group/phs000007.v32.p13-all-subjects"}],
    "sponsor": {
        "reference": "Organization/NHLBI",
        "type": "Organization",
        "display": "National Heart, Lung, and Blood Institute",
    },
}

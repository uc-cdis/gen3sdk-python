"""
Contains captured responses from NIH dbGaP's Study Registration server
to be used in testing.
"""

"""
Has child studies
"""
MOCK_PHS001172 = """
<?xml version="1.0" encoding="UTF-8" ?>
<dbgapss xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="dbgapssws.1.0.xsd">
    <Study uid="20384" whole_study_id="20384" phs="001172" v="1" p="2" createDate="2016-06-29T13:02:33-05:00"
           completedByGPADate="2016-06-29T16:52:39-05:00" modDate="2019-07-19T13:31:34-05:00"
           maxParentChildStudyModDate="2019-07-19T13:31:34-05:00" handle="ParkinsonsDisease_SeqGWAS"
           num_participants="">
        <StudyInfo accession="phs001172.v1.p2" parentAccession="phs001172.v1.p2">
            <childAccession>phs000089.v4.p2</childAccession>
            <childAccession>phs001103.v1.p2</childAccession>
            <childId child_whole_study_id="5367" phs="000089" version="4" status="released">10263</childId>
            <childId child_whole_study_id="11719" phs="001103" version="1" status="released">11719</childId>
            <BioProject id="PRJNA330889" entrez_id="330889" type="bp_admin_access"/>
            <BioProject id="PRJNA330890" entrez_id="330890" type="bp_data_submission"/>
            <StudyNameEntrez>NINDS Parkinson&#8217;s Disease</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="Yes"/>
                <StudyType21 name="analysis" chosen="No"/>
                <StudyType21 name="individual_sequencing" chosen="Yes">
                    <StudyType22 name="whole_targ_exome"/>
                </StudyType21>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="Yes"/>
                <StudyType21 name="subject_samples" chosen="Yes"/>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <StudyTypes>
                <StudyType>phenotype_data</StudyType>
                <StudyType>genotype_data</StudyType>
                <StudyType>individual_sequencing</StudyType>
                <StudyType>whole_exome</StudyType>
            </StudyTypes>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="7" name="NINDS" is_funding_ic="true" is_admin_ic="true" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="0011116058" aid="1" auth="cit" login="singleta" fname="Andrew" mname="B"
                        lname="Singleton" email="singleta@mail.nih.gov">
                    <Role>PI</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0013238013" aid="1" auth="cit" login="sutherlandm" fname="Margaret" mname="L"
                        lname="Sutherland" email="sutherlandm@ninds.nih.gov">
                    <Role>PO</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person aid="8" auth="virtual" fname="Jinhui" lname="Ding" email="dingj@mail.nih.gov">
                    <Role>PI_ASSIST</Role>
                    <Organization/>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova"
                        email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0010146585" aid="1" auth="cit" login="zhangr" fname="Ran" mname="" lname="Zhang"
                        email="zhangr@mail.nih.gov">
                    <Role>GPA</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1"
                weeks-to-cancel-request="8" pub-embargo="none">
            <acknowledgement_statement>INSTRUCTIONS: This study has not provided a suggested acknowledgement statement.
                However, Approved Users are expected to acknowledge the Submitting Investigator(s) who submitted data
                from the original study to an NIH-designated data repository, the primary funding organization that
                supported the Submitting Investigator(s), and the NIH-designated data repository (e.g., dbGaP). The
                acknowledgment statement should include the dbGaP accession number to the specific version of the
                dataset(s) analyzed.
            </acknowledgement_statement>
            <ic_specific_access_term/>
            <DUC universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC_PDF>
            <GSR_Access gsr_mode="unrest_access_by_timeout"
                        gsr_mode_label="Unrestricted access (institution did not respond within six months)"/>
            <ConsentGroup uid="2069" CGType="cg_class_GRU" title="General Research Use" name="GRU" dac_uid="53"
                          dac_name="NINDS" irb-approval-required="No">
                <Use-Restriction>Use of the data is limited only by the terms of the model Data Use Certification.
                </Use-Restriction>
            </ConsentGroup>
            <ConsentGroupLinks>
                <ConsentGroupLink cg_uid_from="2069">
                    <StudyTo study_uid="10253"
                             name="dbGaP Collection: Compilation of Individual-Level Genomic Data for General Research Use"
                             accession="phs000688.v1">
                        <ConsentGroupTo uid="1115" name="GRU"/>
                    </StudyTo>
                </ConsentGroupLink>
            </ConsentGroupLinks>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released"/>
    </Study>
</dbgapss>
""".replace(
    "\n", ""
)

"""
Does not have child studies 
"""
MOCK_PHS001173 = """
<?xml version="1.0" encoding="UTF-8" ?>
<dbgapss xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="dbgapssws.1.0.xsd">
    <Study uid="5472" whole_study_id="5472" phs="001173" v="1" p="1" createDate="2009-01-12T09:45:47-05:00"
           completedByGPADate="2016-07-18T18:06:39-05:00" modDate="2019-04-26T14:00:11-05:00"
           maxParentChildStudyModDate="2019-04-26T14:00:11-05:00" handle="HeadNeckCancer_GWAS" num_participants="3440">
        <StudyInfo accession="phs001173.v1.p1" parentAccession="phs001173.v1.p1">
            <BioProject id="PRJNA330887" entrez_id="330887" type="bp_admin_access"/>
            <BioProject id="PRJNA330888" entrez_id="330888" type="bp_data_submission"/>
            <StudyNameReportPage/>
            <StudyNameEntrez>Head and Neck Cancer Genome-wide Association Study</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="Yes"/>
                <StudyType21 name="analysis" chosen="Yes"/>
                <StudyType21 name="individual_sequencing" chosen="No"/>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="Yes">
                    <StudyType22 name="array_multi_sample_gen"/>
                </StudyType21>
                <StudyType21 name="subject_samples" chosen="Yes">
                    <StudyType22 name="dna"/>
                </StudyType21>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <StudyTypes>
                <StudyType>phenotype_data</StudyType>
                <StudyType>genotype_data</StudyType>
                <StudyType>dna</StudyType>
                <StudyType>snp_array</StudyType>
                <StudyType>array_derived_genotypes</StudyType>
                <StudyType>analysis</StudyType>
            </StudyTypes>
            <Publications>
                <Publication/>
            </Publications>
            <Funding>R01 CA131324</Funding>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="4" name="NCI" is_funding_ic="false" is_admin_ic="false" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="SSHETE" aid="2" auth="eRA" login="SSHETE" fname="Sanjay" lname="Shete"
                        email="sshete@mdanderson.org">
                    <Role>PI</Role>
                    <Organization>UNIVERSITY OF TX MD ANDERSON CAN CTR</Organization>
                </Person>
                <Person nedid="0011508081" aid="1" auth="cit" login="zanettik" fname="Krista" mname="Anne"
                        lname="Zanetti" email="zanettik@mail.nih.gov">
                    <Role>PO</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person aid="8" auth="virtual" fname="Robert" lname="Yu" email="rkyu@mdanderson.org">
                    <Role>PI_ASSIST</Role>
                    <Organization/>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova"
                        email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0011559580" aid="1" auth="cit" login="stefanov" fname="Stefan" mname="Alexandrov"
                        lname="Stefanov" email="stefanov@mail.nih.gov">
                    <Role>GENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="2001203526" aid="1" auth="cit" login="cagaananef" fname="Emilie Charlisse"
                        mname="Forones" lname="Caga-Anan" email="charlisse.caga-anan@nih.gov">
                    <Role>GPA</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1"
                weeks-to-cancel-request="8" pub-embargo="none">
            <acknowledgement_statement>Funding support for this study was provided by NCI (R01 CA131324).
            </acknowledgement_statement>
            <ic_specific_access_term/>
            <DUC universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC_PDF>
            <GSR_Access gsr_mode="unrest_access_by_timeout"
                        gsr_mode_label="Unrestricted access (institution did not respond within six months)"/>
            <ConsentGroup uid="2106" CGType="cg_class_GRU" title="General Research Use" name="GRU" dac_uid="59"
                          dac_name="NCI DAC" irb-approval-required="No">
                <Use-Restriction>Use of the data is limited only by the terms of the model Data Use Certification.
                </Use-Restriction>
            </ConsentGroup>
            <ConsentGroupLinks>
                <ConsentGroupLink cg_uid_from="2106">
                    <StudyTo study_uid="47917" name="NCI's Datasets for General Research Use" accession="phs003014.v1">
                        <ConsentGroupTo uid="5327" name="GRU"/>
                    </StudyTo>
                </ConsentGroupLink>
            </ConsentGroupLinks>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released"/>
    </Study>
</dbgapss>
""".replace(
    "\n", ""
)

"""
Has multiple studies
"""
MOCK_PHS001174 = """
<?xml version="1.0" encoding="UTF-8" ?>
<dbgapss xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="dbgapssws.1.0.xsd">
    <Study uid="5472" whole_study_id="5472" phs="001174" v="1" p="1" createDate="2009-01-12T09:45:47-05:00"
           completedByGPADate="2016-07-18T18:06:39-05:00" modDate="2019-04-26T14:00:11-05:00"
           maxParentChildStudyModDate="2019-04-26T14:00:11-05:00" handle="HeadNeckCancer_GWAS" num_participants="3440">
        <StudyInfo accession="phs001174.v1.p1" parentAccession="phs001174.v1.p1">
            <BioProject id="PRJNA330887" entrez_id="330887" type="bp_admin_access"/>
            <BioProject id="PRJNA330888" entrez_id="330888" type="bp_data_submission"/>
            <StudyNameReportPage/>
            <StudyNameEntrez>Head and Neck Cancer Genome-wide Association Study</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="Yes"/>
                <StudyType21 name="analysis" chosen="Yes"/>
                <StudyType21 name="individual_sequencing" chosen="No"/>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="Yes">
                    <StudyType22 name="array_multi_sample_gen"/>
                </StudyType21>
                <StudyType21 name="subject_samples" chosen="Yes">
                    <StudyType22 name="dna"/>
                </StudyType21>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <StudyTypes>
                <StudyType>phenotype_data</StudyType>
                <StudyType>genotype_data</StudyType>
                <StudyType>dna</StudyType>
                <StudyType>snp_array</StudyType>
                <StudyType>array_derived_genotypes</StudyType>
                <StudyType>analysis</StudyType>
            </StudyTypes>
            <Publications>
                <Publication/>
            </Publications>
            <Funding>R01 CA131324</Funding>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="4" name="NCI" is_funding_ic="false" is_admin_ic="false" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="SSHETE" aid="2" auth="eRA" login="SSHETE" fname="Sanjay" lname="Shete"
                        email="sshete@mdanderson.org">
                    <Role>PI</Role>
                    <Organization>UNIVERSITY OF TX MD ANDERSON CAN CTR</Organization>
                </Person>
                <Person nedid="0011508081" aid="1" auth="cit" login="zanettik" fname="Krista" mname="Anne"
                        lname="Zanetti" email="zanettik@mail.nih.gov">
                    <Role>PO</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person aid="8" auth="virtual" fname="Robert" lname="Yu" email="rkyu@mdanderson.org">
                    <Role>PI_ASSIST</Role>
                    <Organization/>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova"
                        email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0011559580" aid="1" auth="cit" login="stefanov" fname="Stefan" mname="Alexandrov"
                        lname="Stefanov" email="stefanov@mail.nih.gov">
                    <Role>GENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="2001203526" aid="1" auth="cit" login="cagaananef" fname="Emilie Charlisse"
                        mname="Forones" lname="Caga-Anan" email="charlisse.caga-anan@nih.gov">
                    <Role>GPA</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1"
                weeks-to-cancel-request="8" pub-embargo="none">
            <acknowledgement_statement>Funding support for this study was provided by NCI (R01 CA131324).
            </acknowledgement_statement>
            <ic_specific_access_term/>
            <DUC universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC_PDF>
            <GSR_Access gsr_mode="unrest_access_by_timeout"
                        gsr_mode_label="Unrestricted access (institution did not respond within six months)"/>
            <ConsentGroup uid="2106" CGType="cg_class_GRU" title="General Research Use" name="GRU" dac_uid="59"
                          dac_name="NCI DAC" irb-approval-required="No">
                <Use-Restriction>Use of the data is limited only by the terms of the model Data Use Certification.
                </Use-Restriction>
            </ConsentGroup>
            <ConsentGroupLinks>
                <ConsentGroupLink cg_uid_from="2106">
                    <StudyTo study_uid="47917" name="NCI's Datasets for General Research Use" accession="phs003014.v1">
                        <ConsentGroupTo uid="5327" name="GRU"/>
                    </StudyTo>
                </ConsentGroupLink>
            </ConsentGroupLinks>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released"/>
    </Study>
     <Study uid="5472" whole_study_id="5472" phs="001174" v="1" p="1" createDate="2009-01-12T09:45:47-05:00"
           completedByGPADate="2016-07-18T18:06:39-05:00" modDate="2019-04-26T14:00:11-05:00"
           maxParentChildStudyModDate="2019-04-26T14:00:11-05:00" handle="HeadNeckCancer_GWAS" num_participants="3440">
        <StudyInfo accession="phs001174.v1.p1" parentAccession="phs001174.v1.p1">
            <BioProject id="PRJNA330887" entrez_id="330887" type="bp_admin_access"/>
            <BioProject id="PRJNA330888" entrez_id="330888" type="bp_data_submission"/>
            <StudyNameReportPage/>
            <StudyNameEntrez>Head and Neck Cancer Genome-wide Association Study</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="Yes"/>
                <StudyType21 name="analysis" chosen="Yes"/>
                <StudyType21 name="individual_sequencing" chosen="No"/>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="Yes">
                    <StudyType22 name="array_multi_sample_gen"/>
                </StudyType21>
                <StudyType21 name="subject_samples" chosen="Yes">
                    <StudyType22 name="dna"/>
                </StudyType21>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <StudyTypes>
                <StudyType>phenotype_data</StudyType>
                <StudyType>genotype_data</StudyType>
                <StudyType>dna</StudyType>
                <StudyType>snp_array</StudyType>
                <StudyType>array_derived_genotypes</StudyType>
                <StudyType>analysis</StudyType>
            </StudyTypes>
            <Publications>
                <Publication/>
            </Publications>
            <Funding>R01 CA131324</Funding>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="4" name="NCI" is_funding_ic="false" is_admin_ic="false" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="SSHETE" aid="2" auth="eRA" login="SSHETE" fname="Sanjay" lname="Shete"
                        email="sshete@mdanderson.org">
                    <Role>PI</Role>
                    <Organization>UNIVERSITY OF TX MD ANDERSON CAN CTR</Organization>
                </Person>
                <Person nedid="0011508081" aid="1" auth="cit" login="zanettik" fname="Krista" mname="Anne"
                        lname="Zanetti" email="zanettik@mail.nih.gov">
                    <Role>PO</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person aid="8" auth="virtual" fname="Robert" lname="Yu" email="rkyu@mdanderson.org">
                    <Role>PI_ASSIST</Role>
                    <Organization/>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova"
                        email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0011559580" aid="1" auth="cit" login="stefanov" fname="Stefan" mname="Alexandrov"
                        lname="Stefanov" email="stefanov@mail.nih.gov">
                    <Role>GENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="2001203526" aid="1" auth="cit" login="cagaananef" fname="Emilie Charlisse"
                        mname="Forones" lname="Caga-Anan" email="charlisse.caga-anan@nih.gov">
                    <Role>GPA</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1"
                weeks-to-cancel-request="8" pub-embargo="none">
            <acknowledgement_statement>Funding support for this study was provided by NCI (R01 CA131324).
            </acknowledgement_statement>
            <ic_specific_access_term/>
            <DUC universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf"
                      filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC_PDF>
            <GSR_Access gsr_mode="unrest_access_by_timeout"
                        gsr_mode_label="Unrestricted access (institution did not respond within six months)"/>
            <ConsentGroup uid="2106" CGType="cg_class_GRU" title="General Research Use" name="GRU" dac_uid="59"
                          dac_name="NCI DAC" irb-approval-required="No">
                <Use-Restriction>Use of the data is limited only by the terms of the model Data Use Certification.
                </Use-Restriction>
            </ConsentGroup>
            <ConsentGroupLinks>
                <ConsentGroupLink cg_uid_from="2106">
                    <StudyTo study_uid="47917" name="NCI's Datasets for General Research Use" accession="phs003014.v1">
                        <ConsentGroupTo uid="5327" name="GRU"/>
                    </StudyTo>
                </ConsentGroupLink>
            </ConsentGroupLinks>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released"/>
    </Study>
</dbgapss>
""".replace(
    "\n", ""
)

MOCK_BAD_RESPONSE = """
{
    "
}
""".replace(
    "\n", ""
)

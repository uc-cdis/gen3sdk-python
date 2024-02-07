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

MOCK_PHS000089 = """
<?xml version="1.0" encoding="UTF-8" ?>
<dbgapss xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="dbgapssws.1.0.xsd">
    <Study uid="10263" whole_study_id="5367" phs="000089" v="4" p="2" createDate="2013-12-05T16:10:16-05:00" completedByGPADate="2014-03-12T12:28:13-05:00" modDate="2016-11-03T01:08:52-05:00" maxParentChildStudyModDate="2019-07-19T13:31:34-05:00" targetDataDeliveryDate="N/A" targetPublicReleaseDate="2007-10-12" handle="ParkinsonsDisease" num_participants="535">
        <StudyInfo accession="phs000089.v4.p2" parentAccession="phs001172.v1.p2" parentId="20384" parent_whole_study_id="20384">
            <BioProject id="PRJNA75681" entrez_id="75681" type="bp_admin_access"/>
            <BioProject id="PRJNA75683" entrez_id="75683" type="bp_data_submission"/>
            <StudyNameReportPage/>
            <StudyNameEntrez>NINDS-Genome-Wide Genotyping in Parkinson's Disease</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="Yes"/>
                <StudyType21 name="analysis" chosen="No"/>
                <StudyType21 name="individual_sequencing" chosen="No"/>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="No"/>
                <StudyType21 name="subject_samples" chosen="Yes"/>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <StudyTypes>
                <StudyType>phenotype_data</StudyType>
            </StudyTypes>
            <Publications>
                <Publication>
                    <Unparsed>Fung HC, Scholz S, Matarin M, Sim??hez J, Hernandez D, Britton A, Gibbs JR, Langefeld C, Stiegert ML, Schymick J, Okun MS, Mandel RJ, Fernandez HH, Foote KD, Rodr?ez RL, Peckham E, De Vrieze FW, Gwinn-Hardy K, Hardy JA, Singleton A. 
Genome-wide genotyping in Parkinson's disease and neurologically normal controls: first stage analysis and public release of data. 
Lancet Neurol. 2006 Nov; 5(11):911-6. 


Simon-Sanchez J, Scholz S, Fung HC, Matarin M, Hernandez D, Gibbs JR, Britton A, de Vrieze FW, Peckham E, Gwinn-Hardy K, Crawley A, Keen JC, Nash J, Borgaonkar D, Hardy J, Singleton A. 
Genome-wide SNP assay reveals structural genomic variation, extended homozygosity and cell-line induced alterations in normal individuals. 
Hum Mol Genet. 2007 Jan 1; 16(1):1-14. </Unparsed>
                </Publication>
            </Publications>
            <Funding>Z01 AG000949-02</Funding>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="7" name="NINDS" is_funding_ic="true" is_admin_ic="true" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="0011116058" aid="1" auth="cit" login="singleta" fname="Andrew" mname="B" lname="Singleton" email="singleta@mail.nih.gov">
                    <Role>PI</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0013238013" aid="1" auth="cit" login="sutherlandm" fname="Margaret" mname="L" lname="Sutherland" email="sutherlandm@ninds.nih.gov">
                    <Role>PO</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova" email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0011552437" aid="1" auth="cit" login="jawang" fname="Zhen" mname="Yuan" lname="Wang" email="zhen.wang@nih.gov">
                    <Role>GENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0010146585" aid="1" auth="cit" login="zhangr" fname="Ran" mname="" lname="Zhang" email="zhangr@mail.nih.gov">
                    <Role>GPA</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1" weeks-to-cancel-request="8" pub-embargo="none">
            <acknowledgement_statement>INSTRUCTIONS:  This study has not provided a suggested acknowledgement statement. However, Approved Users are expected to acknowledge the Submitting Investigator(s) who submitted data from the original study to an NIH-designated data repository, the primary funding organization that supported the Submitting Investigator(s), and the NIH-designated data repository (e.g., dbGaP).  The acknowledgment statement should include the dbGaP accession number to the specific version of the dataset(s) analyzed.</acknowledgement_statement>
            <ic_specific_access_term/>
            <DUC universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf" filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf" filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC_PDF>
            <GSR_Access gsr_mode="unrest_access_by_timeout" gsr_mode_label="Unrestricted access (institution did not respond within six months)"/>
            <ConsentGroup uid="2069" CGType="cg_class_GRU" title="General Research Use" name="GRU" dac_uid="53" dac_name="NINDS" irb-approval-required="No">
                <Use-Restriction>Use of the data is limited only by the terms of the model Data Use Certification.

</Use-Restriction>
            </ConsentGroup>
            <ConsentGroupLinks>
                <ConsentGroupLink cg_uid_from="2069">
                    <StudyTo study_uid="10253" name="dbGaP Collection: Compilation of Individual-Level Genomic Data for General Research Use" accession="phs000688.v1">
                        <ConsentGroupTo uid="1115" name="GRU"/>
                    </StudyTo>
                </ConsentGroupLink>
            </ConsentGroupLinks>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released" release_date="2016-12-22T22:05:15-05:00"/>
    </Study>
    <Study uid="6062" whole_study_id="5367" phs="000089" v="3" p="2" createDate="2010-03-29T16:43:05-05:00" modDate="2020-09-01T09:55:24-05:00" maxParentChildStudyModDate="2020-09-01T09:55:24-05:00" targetDataDeliveryDate="N/A" targetPublicReleaseDate="2007-10-12" handle="NINDS_ParkinsonsDisease" num_participants="535">
        <StudyInfo accession="phs000089.v3.p2" parentAccession="phs000089.v3.p2">
            <BioProject id="PRJNA75681" entrez_id="75681" type="bp_admin_access"/>
            <BioProject id="PRJNA75683" entrez_id="75683" type="bp_data_submission"/>
            <StudyNameReportPage/>
            <StudyNameEntrez>NINDS-Genome-Wide Genotyping in Parkinson's Disease: First Stage Analysis and Public Release of Data</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="Yes"/>
                <StudyType21 name="analysis" chosen="No"/>
                <StudyType21 name="individual_sequencing" chosen="No"/>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="No"/>
                <StudyType21 name="subject_samples" chosen="Yes"/>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <StudyTypes>
                <StudyType>phenotype_data</StudyType>
            </StudyTypes>
            <Description>&lt;p &gt;Epidemiological studies have estimated a cumulative prevalence of PD of greater than 1 per thousand. When prevalence is 
 limited to senior populations, this proportion increases nearly 10-fold. The estimated genetic risk ratio for PD is 
 approximately 1.7 (70% increased risk for PD if a sibling has PD) for all ages, and increases over 7-fold for those under 
 age 66 years. The role for genes contributing to the risk of PD is therefore significant.&lt;/p &gt;&lt;p &gt;This study utilized the well characterized &lt;a href="./study.cgi?id=phs000003"&gt;collection of North American Caucasians 
 with Parkinson &amp;#39;s disease &lt;/a &gt;, and &lt;a href="./study.cgi?id=phs000004"&gt;neurologically normal controls &lt;/a &gt;from the sample 
 population which are banked in the National Institute of Neurological Disorders and Stroke (NINDS Repository) collection 
 for a first stage whole genome analysis.  Genome-wide, single nucleotide polymorphism (SNP) genotyping of these publicly 
 available samples was originally done in 267 Parkinson &amp;#39;s disease patients and 270 controls, and this has been extended 
 to include genome wide genotyping in 939 Parkinson &amp;#39;s disease cases and 802 controls.&lt;/p &gt;&lt;p &gt;The NINDS repository was established in 10-2001 towards the goal of developing standardized, broadly useful diagnostic 
 and other clinical data and a collection of DNA and cell line samples to further advances in gene discovery of neurological 
 disorders. All samples, phenotypic, and genotypic data are available to the research community including to academics and 
 industry scientists. In addition, well characterized neurologically normal control subjects are a part of the collection.  
 This collection formed the basis of this first stage study by Fung et al., and the expanded study by Simon-Sanchez et al. 
 The genotyping data was generated and provided by the laboratory of Dr. Andrew Singleton NIA, and Dr. John Hardy 
 NIA (NIH Intramural, funding from NIA and NINDS).&lt;/p &gt;&lt;p &gt;&lt;b &gt;Important links to apply for individual-level data &lt;/b &gt;&lt;ol &gt;&lt;li &gt;&lt;a href="http://dbgap.ncbi.nlm.nih.gov/aa/wga.cgi?view_pdf &amp;stacc=phs000089.v3.p2" target="_blank"&gt;Data Use Certification Requirements (DUC)&lt;/a &gt;&lt;/li &gt;&lt;li &gt;&lt;a href="http://view.ncbi.nlm.nih.gov/dbgap-controlled" target="_blank"&gt;Apply here for controlled access to individual level data &lt;/a &gt;&lt;/li &gt;&lt;li &gt;&lt;a href="GetPdf.cgi?id=phd000577" target="_blank"&gt;Participant Protection Policy FAQ &lt;/a &gt;&lt;/li &gt;&lt;/ol &gt;&lt;/p &gt;</Description>
            <Attributions>
                <Header title="Principal Investigators">
                    <AttrName>Hong-Chung Fung</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA, Chang Gung Memorial Hospital, Taiwan, and University College London, UK</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Sonja Scholz</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Sim &#243;n-S &#225;nchez</AttrName>
                    <Institution>Instituto de Biomedicina de Valencia, Spain</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Dena Hernandez</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Angela Britton</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Raphael Gibbs</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Carl Langefeld</AttrName>
                    <Institution>Wake Forest University, Winston-Salem, NC, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Matt L. Stiegert</AttrName>
                    <Institution>Wake Forest University, Winston-Salem, NC, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Jennifer Shymick</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Michael S. Okun</AttrName>
                    <Institution>McKnight Brain Institute, Gainesville, FL, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Ronald J. Mandel</AttrName>
                    <Institution>McKnight Brain Institute, Gainesville, FL, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Hubert H. Fernandez</AttrName>
                    <Institution>McKnight Brain Institute, Gainesville, FL, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Kelly D. Foote</AttrName>
                    <Institution>McKnight Brain Institute, Gainesville, FL, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Ram &#243;n L. Rodr &#237;guez</AttrName>
                    <Institution>McKnight Brain Institute, Gainesville, FL, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Elizabteh Peckham</AttrName>
                    <Institution>National Institute of Neurological Disorders and Stroke, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Fabienne Wavrant DeVrieze</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Katrina Gwinn-Hardy</AttrName>
                    <Institution>National Institute of Neurological Disorders and Stroke, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>John A. Hardy</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Principal Investigators">
                    <AttrName>Andy Singleton</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Robert Brown, Jr</AttrName>
                    <Institution>Massachusetts General Hospital, Boston, MA, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>David Simon</AttrName>
                    <Institution>Massachusetts General Hospital, Boston, MA, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Dennis Dickson</AttrName>
                    <Institution>Mayo Clinic, College of Medicine, Jacksonville, FL, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Matthew Farrer</AttrName>
                    <Institution>Mayo Clinic, College of Medicine, Jacksonville, FL, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Emily Gorbold (POSTCEPT study)</AttrName>
                    <Institution>University of Rochester, Rochester, NY, National Institute of Aging, National Institutes of Health, Bethesda, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Ira Shoulson (POSTCEPT study)</AttrName>
                    <Institution>University of Rochester, Rochester, NY, National Institute of Aging, National Institutes of Health, Bethesda, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>John A. Hardy</AttrName>
                    <Institution>National Institute of Aging, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Laura Marsh</AttrName>
                    <Institution>Johns Hopkins Hospital, Baltimore, MD, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Michael Okun</AttrName>
                    <Institution>McKnight Brain Institute, Gainesville, FL, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Ted Roghstein</AttrName>
                    <Institution>The George Washington University, Washington, DC, USA</Institution>
                </Header>
                <Header title="Investigators Submitting Samples to the NINDS Repository Included in this Study">
                    <AttrName>Zbigniew Wszolek</AttrName>
                    <Institution>Mayo Clinic, College of Medicine, Jacksonville, FL, USA</Institution>
                </Header>
                <Header title="NINDS Repository Resources were developed by:">
                    <AttrName>Fina Nash</AttrName>
                    <Institution>Coriell, Camden, NJ, USA</Institution>
                </Header>
                <Header title="NINDS Repository Resources were developed by:">
                    <AttrName>Rod Corriveau</AttrName>
                    <Institution>Coriell, Camden, NJ, USA</Institution>
                </Header>
                <Header title="NINDS Repository Resources were developed by:">
                    <AttrName>Katrina Gwinn-Hardy</AttrName>
                    <Institution>National Institute of Neurological Disorders and Stroke, National Institutes of Health, Bethesda, MD, USA</Institution>
                </Header>
            </Attributions>
            <StudyURLs>
                <Url name="NINDS, National Institute of Neurological Disorders and Stroke" url="http://www.ninds.nih.gov/disorders/parkinsons_disease/parkinsons_disease.htm"/>
                <Url name="National Institute on Aging Laboratory of Neurogenetics" url="http://www.grc.nia.nih.gov/branches/lng/lngindex.htm"/>
            </StudyURLs>
            <Publications>
                <Publication>
                    <Pubmed pmid="17052657"/>
                </Publication>
                <Publication>
                    <Pubmed pmid="17116639"/>
                </Publication>
                <Publication>
                    <Pubmed pmid="19915575"/>
                </Publication>
                <Publication>
                    <Unparsed>Fung HC, Scholz S, Matarin M, Sim??hez J, Hernandez D, Britton A, Gibbs JR, Langefeld C, Stiegert ML, Schymick J, Okun MS, Mandel RJ, Fernandez HH, Foote KD, Rodr?ez RL, Peckham E, De Vrieze FW, Gwinn-Hardy K, Hardy JA, Singleton A. 
Genome-wide genotyping in Parkinson's disease and neurologically normal controls: first stage analysis and public release of data. 
Lancet Neurol. 2006 Nov; 5(11):911-6. 


Simon-Sanchez J, Scholz S, Fung HC, Matarin M, Hernandez D, Gibbs JR, Britton A, de Vrieze FW, Peckham E, Gwinn-Hardy K, Crawley A, Keen JC, Nash J, Borgaonkar D, Hardy J, Singleton A. 
Genome-wide SNP assay reveals structural genomic variation, extended homozygosity and cell-line induced alterations in normal individuals. 
Hum Mol Genet. 2007 Jan 1; 16(1):1-14. </Unparsed>
                </Publication>
            </Publications>
            <Funding>Z01 AG000949-02</Funding>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="7" name="NINDS" is_funding_ic="false" is_admin_ic="false" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="0011116058" aid="1" auth="cit" login="singleta" fname="Andrew" mname="B" lname="Singleton" email="singleta@mail.nih.gov">
                    <Role>PI</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova" email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0010146585" aid="1" auth="cit" login="zhangr" fname="Ran" mname="" lname="Zhang" email="zhangr@mail.nih.gov">
                    <Role>GPA</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1" weeks-to-cancel-request="8" pub-embargo="none">
            <DUC universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf" filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF universal_duc="Yes">
                <File full_url="https://dbgap.ncbi.nlm.nih.gov/aa/docs/Universal_DUC_20190319-01102023.pdf" filename="Universal_DUC_20190319-01102023.pdf" content-type="application/pdf"/>
            </DUC_PDF>
            <ConsentGroup uid="160" CGType="cg_class_other" title="General research use" name="GRU" dac_uid="53" dac_name="NINDS" irb-approval-required="No">
                <Use-Restriction>These data will be used only for research purposes. They will not be used to determine the individual identity of any person or their relationship to another person.</Use-Restriction>
            </ConsentGroup>
            <ConsentGroupLinks>
                <ConsentGroupLink cg_uid_from="160">
                    <StudyTo study_uid="10253" name="dbGaP Collection: Compilation of Individual-Level Genomic Data for General Research Use" accession="phs000688.v1">
                        <ConsentGroupTo uid="1115" name="GRU"/>
                    </StudyTo>
                </ConsentGroupLink>
            </ConsentGroupLinks>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released" release_date="2011-10-18T13:41:38-05:00"/>
    </Study>
    <Study uid="5367" whole_study_id="5367" phs="000089" v="2" p="2" createDate="2008-09-23T16:38:12-05:00" modDate="2020-09-01T09:13:23-05:00" maxParentChildStudyModDate="2020-09-01T09:13:23-05:00" targetDataDeliveryDate="N/A" targetPublicReleaseDate="2007-10-12" handle="ParkinsonsDisease" num_participants="535">
        <StudyInfo accession="phs000089.v2.p2" parentAccession="phs000089.v2.p2">
            <StudyNameReportPage/>
            <StudyNameEntrez>NINDS-Genome-Wide Genotyping in Parkinson's Disease: First Stage Analysis and Public Release of Data</StudyNameEntrez>
            <StudyTypes2 calculated="Yes">
                <StudyType21 name="phenotype_data" chosen="No"/>
                <StudyType21 name="analysis" chosen="No"/>
                <StudyType21 name="individual_sequencing" chosen="No"/>
                <StudyType21 name="supporting_documents" chosen="No"/>
                <StudyType21 name="images" chosen="No"/>
                <StudyType21 name="molecular" chosen="No"/>
                <StudyType21 name="subject_samples" chosen="No"/>
                <StudyType21 name="links_to_ncbi_dbs" chosen="No"/>
                <StudyType21 name="other_general" chosen="No"/>
            </StudyTypes2>
            <Description>Epidemiological studies have estimated a cumulative prevalence of PD of greater than 1 per thousand. When prevalence is limited to senior populations, this proportion increases nearly 10-fold. The estimated genetic risk ratio for PD is approximately 1.7 (70% increased risk for PD if a sibling has PD) for all ages, and increases over 7-fold for those under age 66 years. The role for genes contributing to the risk of PD is therefore significant.

This study utilized the well characterized collection of North American Caucasians with Parkinson's disease, and neurologically normal controls from the sample population which are banked in the National Institute of Neurological Disorders and Stroke (NINDS Repository) collection for a first stage whole genome analysis. Genome-wide, single nucleotide polymorphism (SNP) genotyping of these publicly available samples was done in 267 Parkinson's disease patients and 270 controls.

The NINDS repository was established in 10-2001 towards the goal of developing standardized, broadly useful diagnostic and other clinical data and a collection of DNA and cell line samples to further advances in gene discovery of neurological disorders. All samples, phenotypic, and genotypic data are available to the research community including to academics and industry scientists. In addition, well characterized neurologically normal control subjects are a part of the collection. This collection formed the basis of this first stage study by Fung et al. The genotyping data was generated and provided by the laboratory of Dr. Andrew Singleton NIA, and Dr. John Hardy NIA (NIH Intramural, funding from NIA and NINDS).

Important links to apply for individual-level data 

Data Use Certification Requirements (DUC) 
Apply here for controlled access to individual level data 
Participant Protection Policy FAQ 

NINDS, National Institute of Neurological Disorders and Stroke 

National Institute on Aging Laboratory of Neurogenetics </Description>
            <StudyHistory/>
            <Diseases>
                <Disease diseaseName="Parkinson Disease "/>
            </Diseases>
            <Publications>
                <Publication>
                    <Unparsed>Fung HC, Scholz S, Matarin M, Sim??hez J, Hernandez D, Britton A, Gibbs JR, Langefeld C, Stiegert ML, Schymick J, Okun MS, Mandel RJ, Fernandez HH, Foote KD, Rodr?ez RL, Peckham E, De Vrieze FW, Gwinn-Hardy K, Hardy JA, Singleton A. 
Genome-wide genotyping in Parkinson's disease and neurologically normal controls: first stage analysis and public release of data. 
Lancet Neurol. 2006 Nov; 5(11):911-6. 


Simon-Sanchez J, Scholz S, Fung HC, Matarin M, Hernandez D, Gibbs JR, Britton A, de Vrieze FW, Peckham E, Gwinn-Hardy K, Crawley A, Keen JC, Nash J, Borgaonkar D, Hardy J, Singleton A. 
Genome-wide SNP assay reveals structural genomic variation, extended homozygosity and cell-line induced alterations in normal individuals. 
Hum Mol Genet. 2007 Jan 1; 16(1):1-14. </Unparsed>
                </Publication>
            </Publications>
            <Funding>Z01 AG000949-02</Funding>
        </StudyInfo>
        <Authority>
            <ICs>
                <IC id="7" name="NINDS" is_funding_ic="false" is_admin_ic="false" primary="true"/>
            </ICs>
            <Persons>
                <Person nedid="0011116058" aid="1" auth="cit" login="singleta" fname="Andrew" mname="B" lname="Singleton" email="singleta@mail.nih.gov">
                    <Role>PI</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0010150700" aid="1" auth="cit" login="tagled" fname="Danilo" mname="A" lname="Tagle" email="tagled@mail.nih.gov">
                    <Role>PO</Role>
                    <Organization>NIH</Organization>
                </Person>
                <Person nedid="0012988688" aid="1" auth="cit" login="popovan" fname="Natalia" mname="V" lname="Popova" email="popovan@ncbi.nlm.nih.gov">
                    <Role>PHENO_CURATOR</Role>
                    <Organization>NIH</Organization>
                </Person>
            </Persons>
        </Authority>
        <Policy display-research-statement="true" display-public-summary="true" years-until-renewal="1" weeks-to-cancel-request="8" pub-embargo="none">
            <DUC>
                <File uri="dbgapssws.cgi?blob_id=5369&amp;file=get&amp;page=file" filename="C:\Documents and Settings\\feolo\My Documents\dbGaP\SubmissionSystem\DUCs\\NINDS_PD_v1_DUC.pdf" size="63816" modDate="2008-09-23T16:43:53-05:00" content-type="application/pdf"/>
            </DUC>
            <DUC_PDF>
                <File uri="dbgapssws.cgi?blob_id=5369&amp;file=get&amp;page=file" filename="C:\Documents and Settings\\feolo\My Documents\dbGaP\SubmissionSystem\DUCs\\NINDS_PD_v1_DUC.pdf" size="63816" modDate="2008-09-23T16:43:53-05:00" content-type="application/pdf"/>
            </DUC_PDF>
            <ConsentGroup uid="10" CGType="cg_class_other" title="General research use" name="GRU" dac_uid="53" dac_name="NINDS" irb-approval-required="No">
                <Use-Restriction>These data will be used only for research purposes. They will not be used to determine the individual identity of any person or their relationship to another person.</Use-Restriction>
            </ConsentGroup>
        </Policy>
        <SRA/>
        <Documents/>
        <Status uid="1" name="released" title="Released" release_date="N/A"/>
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

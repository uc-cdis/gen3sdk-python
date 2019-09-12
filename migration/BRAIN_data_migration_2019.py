# Data Migration for BRAIN Commons (Sept 2019)
import subprocess, json, sys
import pandas as pd

#import gen3
#from gen3.auth import Gen3Auth
#from gen3.submission import Gen3Submission

sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/gen3')
from submission import Gen3Submission
from auth import Gen3Auth

sys.path.insert(1, '/Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/expansion')
from expansion import Gen3Expansion

################################################################################
#### Prepare Python Environment

# Data Staging Environment
api = 'https://data-staging.braincommons.org/'
profile = 'brain-staging'
creds = '/Users/christopher/Downloads/brain-staging-credentials.json'
auth = Gen3Auth(api,refresh_file=creds)
sub = Gen3Submission(api,auth)
exp = Gen3Expansion(api, auth)

# Production Environment
prod_api = 'https://data.braincommons.org/'
prod_profile = 'bc'
prod_creds = '/Users/christopher/Downloads/bc-credentials.json'
prod_auth = Gen3Auth(prod_api,refresh_file=prod_creds)
prod_sub = Gen3Submission(prod_api,prod_auth)
prod_exp = Gen3Expansion(prod_api, prod_auth)

# get the old and new data dictionaries
dd = sub.get_dictionary_all()
prod_dd = prod_sub.get_dictionary_all()
# with open ('BRAIN_dd_v077.txt', 'w') as dd_file:
#     dd_txt = json.dumps(dd)
#     dd_file.write(dd_txt)
# with open ('/Users/christopher/Documents/Notes/BHC/data_migration_2019/project_tsvs_migrated/BRAIN_dd_v077.txt', 'r') as dd_file:
#     lines = dd_file.readlines()
#     dd = json.loads(lines[0])

static_case = ['age_at_enrollment','age_at_enrollment_gt89','age_of_immigration',
'days_to_birth','days_to_immigration','ethnicity','ethnicity_other','gender',
'handedness','military_branch','place_of_birth','race',
'race_not_specified_comment','race_other','year_of_birth','age_pd_diagnosis']

all_links_to_drop = {'diagnosis':['cases'],'demographic':['cases'],'app_checkup':['cases'],'assess_need_symptomatic_therapy':['cases'],'assessment_of_depression':['cases'],'beck_depression_inventory':['cases'],'benton_judgement_line_orientation':['cases'],'ces_depression_scale':['cases'],'clinical_checkup':['cases'],'clinical_cognitive_categorization':['cases'],'clinical_global_impression':['cases'],'diagnostic_features_in_pd':['cases'],'digit_span_test':['cases'],'enrollment':['cases'],'epworth_sleepiness_scale':['cases'],'frontal_assessment_battery':['cases'],'geriatric_depression_scale':['cases'],'hamilton_depression_rating':['cases'],'hopkins_verbal_learning_test':['cases'],'impulsive_compulsive_disorder':['cases'],'injury_or_illness':['cases'],'letter_number_sequencing':['cases'],'mds_unified_pd_rating':['cases'],'mini_mental_status_exam':['cases'],'modified_rankin_scale':['cases'],'modified_schwab_england_scale':['cases'],'montreal_cognitive_functional_test':['cases'],'neuropsychological_assessment':['cases'],'new_dot_test':['cases'],'odd_man_out_test':['cases'],'parkinsons_disease_features':['cases'],'physical_activity_scale':['cases'],'purdue_pegboard_test':['cases'],'quality_of_life_scale':['cases'],'recall_and_recognition':['cases'],'rem_behavior_disorder':['cases'],'rem_sleep_behavior':['cases'],'scales_for_outcomes_in_pd':['cases'],'self_report_questionnaire':['cases'],'sensor_checkup':['cases'],'short_form_health_survey':['cases'],'srq_for_pd':['cases'],'state_trait_anxiety_inventory':['cases'],'symbol_digit_modalities_test':['cases'],'symptomatic_therapy':['cases'],'tap_pd_test':['cases'],'total_functional_capacity':['cases'],'unified_parkinsons_disease_rating':['cases'],'upenn_smell_test':['cases'],'webster_step_second_test':['cases'],'adverse_event':['cases', 'treatments', 'clinical_lab_tests', 'surgeries', 'medications'],'allergy': ['cases', 'treatments', 'medications'],'blindness_evaluation': ['cases', 'followups'],'clinical_diagnosis_management': ['cases' 'diagnoses', 'followups'],'compliance': ['cases', 'treatments', 'medications'],'enzyme_activity_assay': ['cases', 'aliquots'],'exposure': ['cases', 'diagnoses', 'followups'],'family_history': ['cases', 'diagnoses'],'fluorescent_microscopy': ['cases', 'aliquots'],'genotyping_result': ['cases', 'aliquots'],'immunoassay': ['cases', 'aliquots'],'medication': ['cases', 'diagnoses', 'followups'],'metabolomics_result': ['cases', 'aliquots'],'neurological_exam': ['cases', 'followups', 'diagnoses'],'physical_exam': ['cases', 'followups', 'diagnoses'],'reproductive_health': ['cases', 'followups', 'diagnoses'],'sample': ['cases', 'followups', 'diagnoses'],'surgery': ['cases', 'followups'],'symptom':['cases', 'followups', 'diagnoses'],'clinical_lab_test':['cases', 'aliquots'],'expression_result':['cases', 'aliquots'],'endpoint':['cases'],'diagnostic_check_sheet_crf':['cases', 'diagnoses'],'aggregated_snp_array':['visits'],'chromatography':['metabolomics_results'],'copy_number_variation':['read_groups'],'exon_expression_file':['expression_results'],'gene_expression_file':['genotyping_results', 'expression_results'],'treatment':['diagnoses'], 'incident':['treatments'],'mass_cytometry_file':['metabolomics_results'],'sequencing_assay':['submitted_unaligned_reads_files','submitted_aligned_reads_files'],'simple_germline_variation':['read_groups'],'snp_array_variation':['genotyping_results'],'submitted_aligned_reads':['genotyping_results', 'read_groups'],'submitted_expression_array':['expression_results'],'submitted_methylation':['expression_results','genotyping_results'],'submitted_snp_array':['genotyping_results'],'submitted_unaligned_reads':['genotyping_results'],'transcript_expression_file':['expression_results'],'clinical_diagnosis_management':['diagnoses','cases']}

%run /Users/christopher/Documents/GitHub/cgmeyer/gen3sdk-python/migration/migration.py
mig = Gen3Migration(api,auth)

########################################################################################################################
# List of Projects to migrate

#project_id = 'mjff-DATATOP' # DONE
#project_id = 'mjff-BioFIND' # DONE
#project_id = 'mjff-Clinician_Input_Study' # DONE
#project_id = 'mjff-DeNoPa' # DONE
#project_id = 'mjff-FS1' # DONE
#project_id = 'mjff-FSTOO'# DONE
#project_id = 'mjff-Raw_PRO_Mini_Trial' # DONE
#project_id = 'mjff-Real_PD' # DONE
#project_id = 'mjff-S4' # DONE
#project_id = 'mjff-LRRK2L' # DONE
#project_id = 'mjff-LRRK2C' # DONE
#project_id = 'mjff-PPMI' # DONE

# Download structured data from all projects as TSV files
#d = prod_exp.get_project_tsvs()

#project_dir = "/Users/christopher/Documents/Notes/BHC/data_migration_2019/project_tsvs_migrated/{}_tsvs".format(project_id)
#cd $project_dir
#!echo $PWD


################################################################################
# Create copies of the original TSVs to modify
d = mig.make_temp_files(prefix='mjff',suffix='tsv')

################################################################################
# Reformat Clinical Data for breaking changes
#demographic to case
df = mig.move_properties(project_id=project_id,from_node='demographic',to_node='case',properties=static_case,dd=dd,parent_node=None)
df = mig.drop_properties(project_id=project_id,node='demographic',properties=static_case+['age_at_onset'])
# clinical_lab_test, 3 breaking changes:
df = mig.drop_properties(project_id=project_id,node='clinical_lab_test',properties=['blood_and_urine_samples_collected']) # 1) Remove column 'blood_and_urine_samples_collected'.
df = mig.change_enum(project_id=project_id,node='clinical_lab_test',prop='test_type',enums={'Prothrombin Time':'null'}) # 2) In column 'test_type': 'replace', "Prothrombin Time" with null ("").
df = mig.change_enum(project_id=project_id,node='clinical_lab_test',prop='test_units',enums={'Coefficient of Variance (PCT)':'Pct'}) # 3) In column 'test_units' change all "Coefficient of Variance (PCT)" to "Pct".
# diagnostic_check_sheet_crf, 1 breaking change:
df = mig.change_enum(project_id=project_id,node='diagnostic_check_sheet_crf',prop='dcs06_if_asymmetric_was_it_unilateral_onset',enums={'true':'Yes','True':'Yes','TRUE':'Yes','false':'No','FALSE':'No','False':'No'}) # Change column 'dcs06_if_asymmetric_was_it_unilateral_onset' from boolean to enum: change 'Yes'=true, 'No'=false
# endpoint, 8 breaking changes: Change 2 enum values for each of 4 properties:
df = mig.change_enum(project_id=project_id,node='endpoint',prop='pd_threat_to_balance',enums={'Absence Of Postural Reflexes':'Absence Of Postural Reflexes; Would Fall If Not Caught By Examiner','Would Fall If Not Caught By Examiner':'Absence Of Postural Reflexes; Would Fall If Not Caught By Examiner'})
df = mig.change_enum(project_id=project_id,node='endpoint',prop='pd_threat_to_full_time',enums={'Imminent':'Imminent, Will Likely Lose Or Stop Full-Time Work Within Next Month','Will Likely Lose Or Stop Full-Time Work Within Next Month':'Imminent, Will Likely Lose Or Stop Full-Time Work Within Next Month'})
df = mig.change_enum(project_id=project_id,node='endpoint',prop='pd_threat_to_part_time',enums={'Imminent':'Imminent, Will Likely Lose Or Stop Part-Time Work Within Next Month','Will Likely Lose Or Stop Part-Time Work Within Next Month':'Imminent, Will Likely Lose Or Stop Part-Time Work Within Next Month'})
df = mig.change_enum(project_id=project_id,node='endpoint',prop='pd_threat_full_time_homemaking',enums={'Imminent':'Imminent, Will Likely Need To Give Up Full-Time Homemaking Within Next Month','Will Likely Need To Give Up Full-Time Homemaking Within Next Month':'Imminent, Will Likely Need To Give Up Full-Time Homemaking Within Next Month'})
# expression_result, 1 breaking change: property 'test_units' change "Coefficient of Variance (PCT)" to "Pct"
df = mig.change_enum(project_id=project_id,node='expression_result',prop='test_units',enums={'Coefficient of Variance (PCT)':'Pct'})

################################################################################
# 1) Add missing visits to nodes that link to case before dropping links
# For BRAIN 2019, don't include imaging nodes (imaging_exam / imaging_file), as these are handled separately
data = mig.batch_add_visits(project_id=project_id,links=all_links_to_drop,new_dd=dd,old_dd=prod_dd)
data = mig.batch_drop_links(project_id=project_id,links=all_links_to_drop)

################################################################################
# IMAGING nodes
# Reformat imaging_exam node:
imaging_subtypes = ['imaging_fmri_exam','imaging_mri_exam','imaging_spect_exam','imaging_ultrasonography_exam','imaging_xray_exam','imaging_ct_exam','imaging_pet_exam']
df = mig.merge_nodes(project_id=project_id,in_nodes=imaging_subtypes,out_node='imaging_exam')
df = mig.add_missing_links(project_id=project_id,node='imaging_exam',link='visit')
required_props={'visit_label':'Imaging','visit_method':'In-person Visit'}
df = mig.create_missing_links(project_id=project_id,node='imaging_exam',link='visit',old_parent='cases',properties=required_props,new_dd=dd,old_dd=prod_dd)
properties_to_move = ['days_to_preg_serum','days_to_urine_dip','preg_not_required','preg_test_performed','serum_pregnancy_test_performed','serum_pregnancy_test_result','preg_serum_time','urine_pregnancy_dip_performed','urine_pregnancy_dip_result','urine_dip_time']
df = mig.move_properties(project_id=project_id,from_node='imaging_exam',to_node='reproductive_health',properties=properties_to_move,dd=dd,parent_node='visit')
properties_to_change = {'days_to_preg_serum':'days_to_serum_pregnancy_test','days_to_urine_dip':'days_to_urine_pregnancy_dip','preg_not_required':'pregnancy_test_not_required','preg_test_performed':'pregnancy_test_performed','preg_serum_time':'serum_pregnancy_test_time','urine_dip_time':'urine_pregnancy_dip_time'}
df = mig.change_property_names(project_id=project_id,node='reproductive_health',properties=properties_to_change)
df = mig.drop_properties(project_id=project_id,node='imaging_exam',properties=properties_to_move)
df = mig.drop_links(project_id=project_id,node='imaging_exam',links=['followups','diagnoses','cases'])
properties_to_merge = {"image_quality_rating":["spect_image_quality_rating","pet_scan_quality_rating","mri_image_quality_rating"]}
df = mig.merge_properties(project_id=project_id,node='imaging_exam',properties=properties_to_merge)
# imaging_file
imaging_subtype_links = ['imaging_fmri_exams','imaging_mri_exams','imaging_spect_exams','imaging_ultrasonography_exams','imaging_xray_exams','imaging_ct_exams','imaging_pet_exams']
df = mig.merge_links(project_id=project_id,node='imaging_file',link='imaging_exams',links_to_merge=imaging_subtype_links)
df = mig.drop_links(project_id=project_id,node='imaging_file',links=imaging_subtypes)

################################################################################
# Submit TSVs to Staging
# Create the Program / Project
prog,proj = project_id.split('-',1)
data = mig.create_project(program=prog,project=proj)
# get submission order of nodes
suborder = mig.get_submission_order(dd=dd,project_id=project_id,prefix='temp',suffix='tsv')
# drop 'id' columns from headers (the old UUIDs)
data = mig.drop_ids(project_id,suborder)
# Remove any special chars:
# In this case it's a weird apostrophe Rancho is adding to "Parkinson's Disease", so that's the only thing this script checks for at the moment
df = mig.remove_special_chars(project_id,node='case')
df = mig.remove_special_chars(project_id,node='study')

# Submit the TSVs according to suborder
data = mig.submit_tsvs(project_id,suborder,check_done=True)


################################################################################
# RESTARTS /  Fixes for individual files
################################################################################
# PPMI
project_id = 'mjff-PPMI'
node= 'case'
filename="temp_{}_{}.tsv".format(project_id,node)
chunk_size = 1000
row_offset = 0
data = sub.submit_file(project_id,filename,chunk_size,row_offset)

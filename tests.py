# Tests

from gen3 import auth, submission, index
import pprint, json

endpoint = 'https://avantol.planx-pla.net/'
auth = auth.Gen3Auth(endpoint, refresh_file="alexcredentials.json")

def pp(s):
    pprint.pprint(s)

### TESTS FOR SUBMISSION.PY ###
#*****************************#

#sub = submission.Gen3Submission(endpoint, auth)

# print(sub.create_program(
#     {
#         "dbgap_accession_number": 'prog1',
#         "name": "prog1",
#         'type': 'program'
#     }
# ))

# print(sub.create_project(
#     'prog1',
#     {
#         'code': 'proj1',
#         'dbgap_accession_number': 'proj1',
#         'name': 'proj1',
#         'availability_type': 'Open'
#     }
# ))

# print(sub.create_project(
#     'prog1',
#     {
#         'code': 'proj2',
#         'dbgap_accession_number': 'proj2',
#         'name': 'proj2',
#         'availability_type': 'Open'
#     }
# ))

# query = "{program { name }}"
# print(sub.query(query))
# query = "{project { name }}"
# print(sub.query(query))

# query = '{program(name: "prog1") { name, projects {name} }}'
# print(sub.query(query))

# print(sub.delete_program("prog1"))
# print(sub.delete_project(
#     'prog1',
#     'prog1'
# ))
# print(sub.delete_project(
#     'prog1',
#     'proj2'
# ))
# print(sub.submit_record(
#     'prog1', 'proj1',
#     {
#         'projects':[{
#         'code': 'proj1',
#         'dbgap_accession_number': 'proj1',
#         'name': 'proj1'
#     }],
#         'submitter_id':"",
#         "type":'experiment'
#     }
# ))
# print(sub.submit_record(
#     'prog1', 'proj1',
#     {
#         'projects':[{
#         'code': 'proj1',
#         'dbgap_accession_number': 'proj1',
#         'name': 'proj1'
#     }],
#         'submitter_id':"mjm-experiment2",
#         "type":'experiment'
#     }
# ))

# print(sub.delete_record(
#     'prog1', 'proj1', 'c5537056-2722-4056-a113-cf96e717ea67'
# ))
# pp(sub.export_record(
#     'prog1', 'proj1',
#     'c5537056-2722-4056-a113-cf96e717ea67',
#     'tsv'
# ))
# print("---")
# pp(sub.export_node(
#     'prog1', 'proj1', 'experiment', 'json'
# ))

# print(sub.submit_file(
#     'prog1-proj1',
#     'data.xlsx'
# ))

# pp(sub.view_programs())
# pp(sub.view_projects('prog1'))
# #pp(sub.get_project_dictionary('prog1', 'proj1'))
# #pp(sub.get_project_manifest('DEV', 'test'))
# pp(sub.open_project('prog1', 'proj1'))

#*****************************#

### TESTS FOR Index.PY ###
#*****************************#
index = index.Gen3Index(endpoint, auth)

def print_bulk(bulkstring):
    recs = eval(bulkstring.replace("null", "None"))
    print(*recs, sep="\n")

# index.get_status()
# print("-"*30)

# pp(index.get_version())
# print("-"*30)

# pp(index.get_stats())
# print("-"*30)

# pp(index.global_get("0024c784-1e65-465f-aad9-8f1da6aad333"))
# print('-'*30)

pp(index.get_index())
print("-"*30)

# pp(index.get_record("004248db-679d-4421-af83-5b8da226033c"))
# pp(index.get_record("0024c784-1e65-465f-aad9-8f1da6aad333"))
# print("-"*30)
# recs = ["004248db-679d-4421-af83-5b8da226033c",
#          "0024c784-1e65-465f-aad9-8f1da6aad333",
#          "7eb269f0-30de-49bc-85b4-9668917aaa58"]

# print_bulk(index.get_record_bulk(recs))
# print('-'*30)

# pp(index.add_record(
#     {'authz': ['/programs/prog1/projects/proj1'],
#         'hashes': {'md5': 'ab167e49d251118939b1ede427521111'},
#         'size': 0
#         }))
# print("-"*30)
# result: "did":"09006cee-7827-43f1-9e50-6c212b32bc6f","rev":"c65d19a1"

# pp(index.add_new_version(
#     "09006cee-7827-43f1-9e50-6c212b32bc6f", {
#         'hashes': {'md5': '4be2c613d1e5337bb22c0239945d984c'},
#         'size': 1
#         }))
# print("-"*30)
# result: "did":"12302d04-9f8d-46dd-bcc8-a772e4bc959d","rev":"80af47b4"

# pp(index.delete_record("7eb269f0-30de-49bc-85b4-9668917aaa58", 'd4156ac6'))
# print_bulk(index.get_record_bulk(recs))

# pp(index.get_record("09006cee-7827-43f1-9e50-6c212b32bc6f"))
# print("-"*30)
# pp(index.update_record("09006cee-7827-43f1-9e50-6c212b32bc6f", 
#                     "b6520748", 
#                     {"uploader":"mjmartinson",
#                      "file_name":"noname",}))
# print("-"*30)
# pp(index.get_record("09006cee-7827-43f1-9e50-6c212b32bc6f"))
# print("-"*30)
# pp(index.get_record("7f1dbeac-33a7-45f3-9f39-9e387322430e"))
# print("-"*30)
# pp(index.get_latestversion("7f1dbeac-33a7-45f3-9f39-9e387322430e", has_version='true'))
# print("-"*30)
# pp(index.get_versions("12302d04-9f8d-46dd-bcc8-a772e4bc959d"))
# print("-"*30)
# pp(index.create_blank(
#     {'uploader':'MichaelJ', 'file_name':'blank'}))
# print("-"*30)
# pp(index.get_urls(
#     size=3891065,
#     hash="md5:4be2c613d1e5337bbf4c0239945d984c",
#     ids="0024c784-1e65-465f-aad9-8f1da6aad333,6beba55f-f2f3-45e4-9b3c-748a1dc53d76")
#     )
# print("-"*30)
# pp(index.update_blank('guid', 'rev', {body})
# print("-"*30)

#*****************************#

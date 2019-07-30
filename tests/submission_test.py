import pytest, os

def test_view(sub):
    ''' 
    '''
    assert sub.view_programs().status_code == 200

def test_newprog_and_proj(sub):
    ''' Create, get, and delete a program and project

        tests:
        create_program
        delete_program
    '''
    # create a program
    p = sub.create_program(
		    {
		    "dbgap_accession_number": "programmjm",
		    "name": "programmjm",
            'type':'program'
		    }).json()
    assert p.status_code == 200
    # test the existence of the program and the view_projects function
    assert sub.view_projects('programmjm').status_code == 200
    # create a project 
    pj = sub.create_project(
        'programmjm',
        {
            'code': 'projectmjm',
            'dbgap_accession_number': 'projectmjm',
            'name': 'projectmjm',
            'availability_type': 'Open'
        }).json()
    assert pj.status_code == 200
    # test the existence of the project and the function:
    # get_project_dictionary
    assert sub.get_project_dictionary('programmjm', 'projectmjm').status_code == 200

    #delete the project and program
    dpj = sub.delete_project('programjm', 'projectmjm').status_code
    dp = sub.delete_program('programmjm').status_code

    assert dpj and dp == 200 or 204

def test_newrecord(sub):
    ''' Create, export, and delete a record

        tests:
        submit_record
        delete_record
        export_record
    '''
    # create a new record in programs/prog1/projects/proj1
    rec = sub.submit_record(
        'prog1', 'proj1', 
        {
            'projects': [{'code': 'proj1' }],
            'submitter_id': 'mjmartinson',
            'type':'experiment'
        })
    assert rec.status_code == 200
    # export the record
    sub.export_record(
        'prog1', 'proj1',
        rec.json()['entities'][0]['id'], 'json',
        'record_file.json')
    assert os.path.exists('record_file.json')
    os.remove('record_file.json')
    assert not os.path.exists('record_file.json')

    # delete the record 
    resp = sub.delete_record(
        'prog1', 'proj1', rec.json()['entities'][0]['id'])
    assert resp.status_code == 200 or 204


'''Tests ingestion into schema tables: Lab, Subject, Session
    1. Assert length of populating data from __init__
    2. Assert exact matches of inserted data for key tables
'''

__all__ = ['dj_config', 'pipeline', 'lab_csv', 'lab_project_csv', 'lab_user_csv',
           'lab_publications_csv',  'lab_keywords_csv',  'lab_protocol_csv',
           'lab_project_users_csv',  'ingest_lab',  'subjects_csv', 'subjects_part_csv',
           'ingest_subjects', 'sessions_csv', 'ingest_sessions']

from . import (dj_config, pipeline, lab_csv,
               lab_project_csv, lab_user_csv, lab_publications_csv,
               lab_keywords_csv, lab_protocol_csv,
               lab_project_users_csv, ingest_lab,
               subjects_csv, subjects_part_csv, ingest_subjects,
               sessions_csv, ingest_sessions)


def test_ingest_lab(pipeline, ingest_lab,
                    lab_csv, lab_project_csv, lab_protocol_csv):
    """Check length of various lab schema tables"""
    lab = pipeline['lab']
    assert len(lab.Lab()) == 2, f'Check Lab: len={len(lab.Lab())}'
    assert len(lab.LabMembership()) == 5, \
        f'Check LabMembership: len={len(lab.LabMembership())}'
    assert len(lab.User()) == 5, f'Check User: len={len(lab.User())}'
    assert len(lab.UserRole()) == 3, f'Check UserRole: len={len(lab.UserRole())}'
    assert len(lab.Location()) == 2, f'Check Location: len={len(lab.Location())}'
    assert len(lab.Project()) == 2, f'Check Project: len={len(lab.Project())}'
    assert len(lab.ProjectUser()) == 5, \
        f'Check ProjectUser: len={len(lab.ProjectUser())}'
    assert len(lab.Protocol()) == 2, f'Check Protocol: len={len(lab.Protocol())}'
    assert len(lab.ProtocolType()) == 2, \
        f'Check ProtocolType: len={len(lab.ProtocolType())}'

    labs, _ = lab_csv
    for this_lab in labs[1:]:
        lab_values = this_lab.split(",")
        assert (lab.Lab & {'lab': lab_values[0]}
                ).fetch1('lab_name') == lab_values[1]

    projects, _ = lab_project_csv
    for this_project in projects[1:]:
        project_values = this_project.split(",")
        assert (lab.Project & {'project': project_values[0]}
                ).fetch1('project_description') == project_values[1]

    protocols, _ = lab_protocol_csv
    for this_protocol in protocols[1:]:
        protocol_values = this_protocol.split(",")
        assert (lab.Protocol & {'protocol': protocol_values[0]}
                ).fetch1('protocol_type') == protocol_values[1]



def test_ingest_subjects(pipeline, subjects_csv, subjects_part_csv, ingest_subjects):
    """Check length of subject.Subject"""
    subject = pipeline['subject']
    assert len(subject.Subject()) == 3, f'Check Subject: len={len(subject.Subject())}'
    assert len(subject.Subject.Protocol()) == 2, \
        f'Check Subject.Protocol: len={len(subject.Subject.Protocol())}'
    assert len(subject.Subject.User()) == 2, \
        f'Check Subject.User: len={len(subject.Subject.User())}'

    subjects, _ = subjects_csv
    subjects_parts, _ = subjects_part_csv
    for this_subject in subjects[1:]:
        subject_values = this_subject.split(",")
        assert (subject.Subject & {'subject': subject_values[0]}
                ).fetch1('subject_description') == subject_values[3]
    for this_subject in subjects_parts[1:]:
        subject_values = this_subject.split(",")
        assert (subject.Subject.Protocol & {'subject': subject_values[0]}
                ).fetch1('protocol') == subject_values[1]
        assert (subject.Subject.User & {'subject': subject_values[0]}
                ).fetch1('user') == subject_values[2]



def test_ingest_sessions(pipeline, sessions_csv, ingest_sessions):
    """Check length/contents of Session.SessionDirectory"""
    session = pipeline['session']
    assert len(session.Session()) == 3, f'Check Session: len={len(session.Session())}'
    assert len(session.ProjectSession()) == 3, \
        f'Check ProjectSession: len={len(session.ProjectSession())}'

    sessions, _ = sessions_csv
    for sess in sessions[1:]:
        sess = sess.split(",")
        assert (session.SessionDirectory & {'subject': sess[0]}
                & {'session_datetime': sess[2]}).fetch1('session_dir') == sess[3]

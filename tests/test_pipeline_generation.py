'''Test pipeline construction
    1. Assert lab link to within-schema children
    2. Assert lab link to subject
    3. Assert subject link to session
'''

__all__ = ['pipeline']

from . import pipeline


def test_generate_pipeline(pipeline):
    session = pipeline['session']
    subject = pipeline['subject']
    lab = pipeline['lab']

    # test connection Lab->schema children, and Lab->Subject.Lab
    lab_membership, loc_tbl, subject_lab_tbl = \
        lab.Lab.children(as_objects=True)
    assert lab_membership.full_table_name == lab.LabMembership.full_table_name
    assert loc_tbl.full_table_name == lab.Location.full_table_name
    assert subject_lab_tbl.full_table_name == \
        subject.Subject.Lab.full_table_name

    # test connection Subject -> schema children
    _, _, _, _, _, session_tbl, _, subject_line_tbl, subject_protocol_tbl, subject_source_tbl, \
        subject_strain_tbl, subject_user_tbl, subject_cull_tbl, subject_death_tbl,\
        subject_zygotsity_tbl = subject.Subject.children(as_objects=True)
    assert session_tbl.full_table_name == session.Session.full_table_name
    assert subject_line_tbl.full_table_name == subject.Subject.Line.full_table_name
    assert subject_protocol_tbl.full_table_name == \
        subject.Subject.Protocol.full_table_name
    assert subject_source_tbl.full_table_name == subject.Subject.Source.full_table_name
    assert subject_strain_tbl.full_table_name == subject.Subject.Strain.full_table_name
    assert subject_user_tbl.full_table_name == subject.Subject.User.full_table_name
    assert subject_cull_tbl.full_table_name == \
        subject.SubjectCullMethod.full_table_name
    assert subject_death_tbl.full_table_name == subject.SubjectDeath.full_table_name
    assert subject_zygotsity_tbl.full_table_name == subject.Zygosity.full_table_name

    # test connection Subject->Session
    subject_tbl, *_ = session.Session.parents(as_objects=True)
    assert subject_tbl.full_table_name == subject.Subject.full_table_name

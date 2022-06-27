"""Test pipeline construction
    1. Assert lab link to within-schema children
    2. Assert lab link to subject
    3. Assert subject link to session
"""

__all__ = ["pipeline"]

from . import pipeline


def test_generate_pipeline(pipeline):
    session = pipeline["session"]
    genotyping = pipeline["genotyping"]
    subject = pipeline["subject"]
    lab = pipeline["lab"]

    # test connection Lab->schema children, and Lab->Subject.Lab
    lab_children = lab.Lab.children()
    assert lab.LabMembership.full_table_name in lab_children
    assert lab.Location.full_table_name in lab_children
    assert subject.Subject.Lab.full_table_name in lab_children

    # test connection Subject -> schema children
    subj_children_links = subject.Subject.children()
    subj_children_list = [
        subject.Subject.Protocol,
        subject.Subject.User,
        subject.Subject.Line,
        subject.Subject.Strain,
        subject.Subject.Source,
        subject.Subject.Lab,
        subject.SubjectDeath,
        subject.SubjectCullMethod,
        subject.Zygosity,
        session.Session,
        genotyping.BreedingPair.Father,
        genotyping.BreedingPair.Mother,
        genotyping.SubjectLitter,
        genotyping.SubjectCaging,
        genotyping.GenotypeTest,
    ]

    for child in subj_children_list:
        assert (
            child.full_table_name in subj_children_links
        ), f"subject.Subject.children() did not include {child.full_table_name}"

    # test genotyping.Sequence -> other genotyping tables
    genotyping_children = genotyping.Sequence.children()
    assert genotyping.AlleleSequence.full_table_name in genotyping_children
    assert genotyping.GenotypeTest.full_table_name in genotyping_children

    # test connection Subject->Session
    session_parents = session.Session.parents()
    assert subject.Subject.full_table_name in session_parents

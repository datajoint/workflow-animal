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
    lab_membership, loc_tbl, subject_lab_tbl = lab.Lab.children(as_objects=True)
    assert lab_membership.full_table_name == lab.LabMembership.full_table_name
    assert loc_tbl.full_table_name == lab.Location.full_table_name
    assert subject_lab_tbl.full_table_name == subject.Subject.Lab.full_table_name

    # test connection Subject -> schema children
    subj_children_link = subject.Subject.children(as_objects=True)
    subj_children_list = [
        genotyping.BreedingPair.Father,
        genotyping.BreedingPair.Mother,
        genotyping.GenotypeTest,
        genotyping.SubjectCaging,
        genotyping.SubjectLitter,
        session.Session,
        subject.Subject.Lab,
        subject.Subject.Line,
        subject.Subject.Protocol,
        subject.Subject.Source,
        subject.Subject.Strain,
        subject.Subject.User,
        subject.SubjectCullMethod,
        subject.SubjectDeath,
        subject.Zygosity,
    ]

    for child_link, child_list in zip(subj_children_link, subj_children_list):
        assert (
            child_link.full_table_name == child_list.full_table_name
        ), f"subject.Subject.children(): Expected {child_list}, Found {child_link}"

    # test genotyping.Sequence -> other genotyping tables
    geno_allele_tbl, geno_test_tbl = genotyping.Sequence.children(as_objects=True)
    assert geno_allele_tbl.full_table_name == genotyping.AlleleSequence.full_table_name
    assert geno_test_tbl.full_table_name == genotyping.GenotypeTest.full_table_name

    # test connection Subject->Session
    subject_tbl, *_ = session.Session.parents(as_objects=True)
    assert subject_tbl.full_table_name == subject.Subject.full_table_name

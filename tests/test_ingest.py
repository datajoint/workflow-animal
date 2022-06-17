"""Tests ingestion into schema tables: Lab, Subject, Session
    1. Assert length of populating data from __init__
    2. Assert exact matches of inserted data for key tables
"""

__all__ = [
    "dj_config",
    "pipeline",
    "lab_csv",
    "lab_project_csv",
    "lab_user_csv",
    "lab_publications_csv",
    "lab_keywords_csv",
    "lab_protocol_csv",
    "lab_sources_csv",
    "lab_project_users_csv",
    "ingest_lab",
    "subjects_csv",
    "subjects_part_csv",
    "allele_csv",
    "cage_csv",
    "breedingpair_csv",
    "genotype_test_csv",
    "line_csv",
    "strain_csv",
    "zygosity_csv",
    "ingest_subjects",
    "sessions_csv",
    "ingest_sessions",
]

from . import (
    dj_config,
    pipeline,
    lab_csv,
    lab_project_csv,
    lab_user_csv,
    lab_publications_csv,
    lab_keywords_csv,
    lab_protocol_csv,
    lab_project_users_csv,
    lab_source_csv,
    ingest_lab,
    subjects_csv,
    subjects_part_csv,
    allele_csv,
    cage_csv,
    breedingpair_csv,
    genotype_test_csv,
    line_csv,
    strain_csv,
    zygosity_csv,
    ingest_subjects,
    sessions_csv,
    ingest_sessions,
)


def test_ingest_lab(
    pipeline, ingest_lab, lab_csv, lab_project_csv, lab_protocol_csv, lab_source_csv
):
    """Check length of various lab schema tables"""
    lab = pipeline["lab"]
    assert len(lab.Lab()) == 2, f"Check Lab: len={len(lab.Lab())}"
    assert (
        len(lab.LabMembership()) == 5
    ), f"Check LabMembership: len={len(lab.LabMembership())}"
    assert len(lab.User()) == 5, f"Check User: len={len(lab.User())}"
    assert len(lab.UserRole()) == 3, f"Check UserRole: len={len(lab.UserRole())}"
    assert len(lab.Location()) == 2, f"Check Location: len={len(lab.Location())}"
    assert len(lab.Project()) == 2, f"Check Project: len={len(lab.Project())}"
    assert (
        len(lab.ProjectUser()) == 5
    ), f"Check ProjectUser: len={len(lab.ProjectUser())}"
    assert len(lab.Protocol()) == 2, f"Check Protocol: len={len(lab.Protocol())}"
    assert (
        len(lab.ProtocolType()) == 2
    ), f"Check ProtocolType: len={len(lab.ProtocolType())}"

    labs, _ = lab_csv
    for this_lab in labs[1:]:
        lab_values = this_lab.split(",")
        assert (lab.Lab & {"lab": lab_values[0]}).fetch1("lab_name") == lab_values[1]

    projects, _ = lab_project_csv
    for this_project in projects[1:]:
        project_values = this_project.split(",")
        assert (lab.Project & {"project": project_values[0]}).fetch1(
            "project_description"
        ) == project_values[1]

    protocols, _ = lab_protocol_csv
    for this_protocol in protocols[1:]:
        protocol_values = this_protocol.split(",")
        assert (lab.Protocol & {"protocol": protocol_values[0]}).fetch1(
            "protocol_type"
        ) == protocol_values[1]


def test_ingest_subjects(
    pipeline,
    ingest_subjects,
    subjects_csv,
    subjects_part_csv,
    allele_csv,
    cage_csv,
    breedingpair_csv,
    genotype_test_csv,
    line_csv,
    strain_csv,
    zygosity_csv,
):
    """Check lengths of tables, then test 1 value per input csv"""
    subject = pipeline["subject"]
    genotyping = pipeline["genotyping"]

    tables = [
        subject.Subject(),  # 0
        subject.Subject.Protocol(),  # 3
        subject.Strain(),  # 6
        subject.Allele(),  # 7
        subject.Line(),  # 11
        subject.Zygosity(),  # 16
        genotyping.BreedingPair(),  # 17
        genotyping.Cage(),  # 23
        genotyping.GenotypeTest(),  # 25
    ]

    lengths = [
        5,  # 0
        2,  # 3
        2,  # 6
        3,  # 7
        3,  # 11
        6,  # 16
        2,  # 17
        2,  # 23
        4,  # 25
    ]

    csvs = [
        subjects_csv,  # 0
        subjects_part_csv,  # 3
        strain_csv,  # 6
        allele_csv,  # 7
        line_csv,  # 11
        zygosity_csv,  # 16
        breedingpair_csv,  # 17
        cage_csv,  # 23
        genotype_test_csv,  # 25
    ]

    tested_cols = {  # column name, index in csv
        "subject_description": 3,
        "protocol": 1,
        "strain_standard_name": 1,
        "allele": 0,
        "line": 0,
        "zygosity": 2,
        "breeding_pair": 2,
        "cage": 0,
        "genotype_test_id": 2,
    }

    for table, length, csv, (col, idx) in zip(
        tables, lengths, csvs, tested_cols.items()
    ):
        assert len(table) == length, f"Check length of {table.full_table_name}"
        content_raw, _ = csv  # split csv content by comma, keep only indexed item
        csv_values = set([val.split(",")[idx] for val in content_raw[1:]])
        db_values = set(list(table.fetch(col)))  # set for unique vals, csvs have dupes
        assert all(  # Lists weren't equal on testing, so iteraterating
            [c == d for c, d in zip(csv_values, db_values)]
        ), (
            f"CSV doesn't match fetched values for {table.full_table_name}:\n"
            + f"CSV:{csv_values}\n DB:{db_values}"
        )


def test_ingest_sessions(pipeline, sessions_csv, ingest_sessions):
    """Check length/contents of Session.SessionDirectory"""
    session = pipeline["session"]
    assert len(session.Session()) == 2, f"Check Session: len={len(session.Session())}"
    assert (
        len(session.ProjectSession()) == 2
    ), f"Check ProjectSession: len={len(session.ProjectSession())}"

    sessions, _ = sessions_csv
    for sess in sessions[1:]:
        sess = sess.split(",")
        assert (
            session.SessionDirectory
            & {"subject": sess[0]}
            & {"session_datetime": sess[2]}
        ).fetch1("session_dir") == sess[3]

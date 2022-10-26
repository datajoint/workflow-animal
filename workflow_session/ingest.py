import csv
from element_interface.utils import ingest_csv_to_table
from workflow_session.pipeline import lab, subject, session, genotyping


def ingest_lab(
    lab_csv_path: str = "./user_data/lab/labs.csv",
    project_csv_path: str = "./user_data/lab/projects.csv",
    publication_csv_path: str = "./user_data/lab/publications.csv",
    keyword_csv_path: str = "./user_data/lab/keywords.csv",
    protocol_csv_path: str = "./user_data/lab/protocols.csv",
    users_csv_path: str = "./user_data/lab/users.csv",
    project_user_csv_path: str = "./user_data/lab/project_users.csv",
    sources_csv_path: str = "./user_data/lab/sources.csv",
    skip_duplicates: bool = True,
    verbose: bool = True,
):
    """Insert data from a CSVs into their corresponding lab schema tables.

    By default, uses data from workflow_session/user_data/lab/

    Args:
        lab_csv_path (str):            relative path of lab csv
        project_csv_path (str):        relative path of project csv
        publication_csv_path (str):    relative path of publication csv
        keyword_csv_path (str):        relative path of keyword csv
        protocol_csv_path (str):       relative path of protocol csv
        users_csv_path (str):          relative path of users csv
        project_user_csv_path (str):   relative path of project users csv
        sources_csv_path (str):        relative path of sources csv
        skip_duplicates (bool): Default True. See DataJoint `insert` function
        verbose (bool): Print number inserted (i.e., table length change)
    """

    # List with repeats for when mult dj.tables fed by same CSV
    csvs = [
        lab_csv_path,  # 0
        lab_csv_path,  # 1
        project_csv_path,  # 2
        project_csv_path,  # 3
        publication_csv_path,  # 4
        keyword_csv_path,  # 5
        protocol_csv_path,  # 6
        protocol_csv_path,  # 7
        users_csv_path,  # 8
        users_csv_path,  # 9
        users_csv_path,  # 10
        project_user_csv_path,  # 11
        sources_csv_path,  # 12
    ]
    tables = [
        lab.Lab(),  # 0
        lab.Location(),  # 1
        lab.Project(),  # 2
        lab.ProjectSourceCode(),  # 3
        lab.ProjectPublication(),  # 4
        lab.ProjectKeywords(),  # 5
        lab.ProtocolType(),  # 6
        lab.Protocol(),  # 7
        lab.UserRole(),  # 8
        lab.User(),  # 9
        lab.LabMembership(),  # 10
        lab.ProjectUser(),  # 11
        lab.Source(),  # 13
    ]

    ingest_csv_to_table(csvs, tables, skip_duplicates=skip_duplicates, verbose=verbose)


def ingest_subjects(
    subject_csv_path: str = "./user_data/subject/subjects.csv",
    subject_part_csv_path: str = "./user_data/subject/subjects_part.csv",
    allele_csv_path: str = "./user_data/subject/allele.csv",
    cage_csv_path: str = "./user_data/subject/cage.csv",
    breedingpair_csv_path: str = "./user_data/subject/breedingpair.csv",
    genotype_test_csv_path: str = "./user_data/subject/genotype_test.csv",
    line_csv_path: str = "./user_data/subject/line.csv",
    strain_csv_path: str = "./user_data/subject/strain.csv",
    zygosity_csv_path: str = "./user_data/subject/zygosity.csv",
    skip_duplicates: bool = True,
    verbose: bool = True,
):
    """Insert data from a subject csv into corresponding subject schema tables

    By default, uses data from workflow_session/user_data/subject/

    Args:
        subject_csv_path (str):        relative path of csv for subject data
        subject_part_csv_path (str):   relative path of csv for subject part tables
        allele_csv_path (str):         relative path of csv for alleles
        cage_csv_path (str):           relative path of csv for cages
        breedingpair_csv_path (str):   relative path of csv for breeding pairs
        genotype_test_csv_path (str):  relative path of csv for genotype
        line_csv_path (str):           relative path of csv for line
        strain_csv_path (str):         relative path of csv for strain
        zygosity_csv_path (str):       relative path of csv for zygotsky
        skip_duplicates (bool): Default True. See DataJoint `insert` function
        verbose (bool): Print number inserted (i.e., table length change)
    """
    csvs = [
        subject_csv_path,  # 0
        subject_csv_path,  # 1
        subject_csv_path,  # 2
        subject_part_csv_path,  # 3
        subject_part_csv_path,  # 4
        subject_part_csv_path,  # 5
        strain_csv_path,  # 6
        allele_csv_path,  # 7
        allele_csv_path,  # 8
        allele_csv_path,  # 9
        allele_csv_path,  # 10
        line_csv_path,  # 11
        line_csv_path,  # 12
        subject_part_csv_path,  # 13
        subject_part_csv_path,  # 14
        subject_part_csv_path,  # 15
        zygosity_csv_path,  # 16
        breedingpair_csv_path,  # 17
        breedingpair_csv_path,  # 18
        breedingpair_csv_path,  # 19
        breedingpair_csv_path,  # 20
        breedingpair_csv_path,  # 21
        breedingpair_csv_path,  # 22
        cage_csv_path,  # 23
        cage_csv_path,  # 24
        genotype_test_csv_path,  # 25
    ]
    tables = [
        subject.Subject(),  # 0
        subject.SubjectDeath(),  # 1
        subject.SubjectCullMethod(),  # 2
        subject.Subject.Protocol(),  # 3
        subject.Subject.User(),  # 4
        subject.Subject.Lab(),  # 5
        subject.Strain(),  # 6
        subject.Allele(),  # 7
        subject.Allele.Source(),  # 8
        genotyping.Sequence(),  # 9
        genotyping.AlleleSequence(),  # 10
        subject.Line(),  # 11
        subject.Line.Allele(),  # 12
        subject.Subject.Line(),  # 13
        subject.Subject.Strain(),  # 14
        subject.Subject.Source(),  # 15
        subject.Zygosity(),  # 16
        genotyping.BreedingPair(),  # 17
        genotyping.BreedingPair.Father(),  # 18
        genotyping.BreedingPair.Mother(),  # 19
        genotyping.Litter(),  # 20
        genotyping.Weaning(),  # 21
        genotyping.SubjectLitter(),  # 22
        genotyping.Cage(),  # 23
        genotyping.SubjectCaging(),  # 24
        genotyping.GenotypeTest(),  # 25
    ]

    ingest_csv_to_table(csvs, tables, skip_duplicates=skip_duplicates, verbose=verbose)


def ingest_sessions(
    session_csv_path: str = "./user_data/session/sessions.csv",
    skip_duplicates: bool = True,
    verbose: bool = True,
):
    """
    Inserts data from a sessions csv into corresponding session schema tables
    By default, uses data from workflow_session/user_data/session/
        session_csv_path (str):     relative path of session csv
        skip_duplicates (bool): Default True. See DataJoint `insert` function
        verbose (bool): Print number inserted (i.e., table length change)
    """
    csvs = [
        session_csv_path,
        session_csv_path,
        session_csv_path,
        session_csv_path,
        session_csv_path,
    ]
    tables = [
        session.Session(),
        session.SessionDirectory(),
        session.SessionNote(),
        session.ProjectSession(),
        session.SessionExperimenter(),
    ]

    ingest_csv_to_table(csvs, tables, skip_duplicates=skip_duplicates, verbose=verbose)


if __name__ == "__main__":
    ingest_lab()
    ingest_subjects()
    ingest_sessions()

"""
run all tests:
    pytest -sv --cov-report term-missing --cov=workflow_session -p no:warnings tests/
run one test, debug:
    pytest [above options] --pdb tests/tests_name.py -k function_name
"""

import os
import sys
import pytest
import pathlib
import datajoint as dj

__all__ = ["pipeline"]

# ------------------- SOME CONSTANTS -------------------

_tear_down = False
verbose = False

pathlib.Path("./tests/user_data").mkdir(exist_ok=True)
pathlib.Path("./tests/user_data/lab").mkdir(exist_ok=True)
pathlib.Path("./tests/user_data/session").mkdir(exist_ok=True)
pathlib.Path("./tests/user_data/subject").mkdir(exist_ok=True)

# ------------------ GENERAL FUNCTIONS ------------------


def write_csv(content, path):
    """
    General function for writing strings to lines in CSV
    :param path: pathlib PosixPath
    :param content: list of strings, each as row of CSV
    """
    with open(path, "w") as f:
        for line in content:
            f.write(line + "\n")


class QuietStdOut:
    """If verbose set to false, used to quiet tear_down table.delete prints"""

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


# ---------------------- FIXTURES ----------------------


@pytest.fixture(autouse=True)
def dj_config():
    """If dj_local_config exists, load"""
    if pathlib.Path("./dj_local_conf.json").exists():
        dj.config.load("./dj_local_conf.json")
    dj.config["safemode"] = False
    dj.config["database.host"] = os.environ.get("DJ_HOST") or dj.config["database.host"]
    dj.config["database.password"] = (
        os.environ.get("DJ_PASS") or dj.config["database.password"]
    )
    dj.config["database.user"] = os.environ.get("DJ_USER") or dj.config["database.user"]
    dj.config["custom"] = {
        "database.prefix": (
            os.environ.get("DATABASE_PREFIX") or dj.config["custom"]["database.prefix"]
        )
    }
    return


@pytest.fixture
def pipeline():
    """Loads workflow_session.pipeline lab, session, subject"""
    from workflow_session import pipeline

    yield {
        "subject": pipeline.subject,
        "genotyping": pipeline.genotyping,
        "session": pipeline.session,
        "lab": pipeline.lab,
    }

    if _tear_down:
        if verbose:
            pipeline.subject.Subject.delete()
            pipeline.session.Session.delete()
            pipeline.lab.Lab.delete()
        else:
            with QuietStdOut():
                pipeline.subject.Subject.delete()
                pipeline.session.Session.delete()
                pipeline.lab.Lab.delete()


@pytest.fixture
def lab_csv():
    """Create a 'labs.csv' file"""
    lab_content = [
        "lab,lab_name,institution,address,time_zone,location,location_description",
        "LabA,The Example Lab,Example Uni,"
        + "'221B Baker St,London NW1 6XE,UK',UTC+0,"
        + "Example Building,'2nd floor lab dedicated to all "
        + "fictional experiments.'",
        "LabB,The Other Lab,Other Uni,"
        + "'Oxford OX1 2JD, United Kingdom',UTC+0,"
        + "Other Building,'fictional campus dedicated to imaginary"
        + "experiments.'",
    ]
    lab_csv_path = pathlib.Path("./tests/user_data/lab/labs.csv")
    write_csv(lab_content, lab_csv_path)

    yield lab_content, lab_csv_path
    lab_csv_path.unlink()


@pytest.fixture
def lab_project_csv():
    """Create a 'projects.csv' file"""
    lab_project_content = [
        "project,project_description,repository_url,repository_name,codeurl",
        "ProjA,Example project to populate element-lab,"
        + "https://github.com/datajoint/element-lab/,"
        + "element-lab,https://github.com/datajoint/element"
        + "-lab/tree/main/element_lab",
        "ProjB,Other example project to populate element-"
        + "lab,https://github.com/datajoint/element-session"
        + "/,element-session,https://github.com/datajoint/"
        + "element-session/tree/main/element_session",
    ]
    lab_project_csv_path = pathlib.Path("./tests/user_data/lab/projects.csv")
    write_csv(lab_project_content, lab_project_csv_path)

    yield lab_project_content, lab_project_csv_path
    lab_project_csv_path.unlink()


@pytest.fixture
def lab_project_users_csv():
    """Create a 'project_users.csv' file"""
    lab_project_user_content = [
        "user,project",
        "Sherlock,ProjA",
        "Sherlock,ProjB",
        "Watson,ProjB",
        "Dr. Candace Pert,ProjA",
        "User1,ProjA",
    ]
    lab_project_user_csv_path = pathlib.Path("./tests/user_data/lab/project_users.csv")
    write_csv(lab_project_user_content, lab_project_user_csv_path)

    yield lab_project_user_content, lab_project_user_csv_path
    lab_project_user_csv_path.unlink()


@pytest.fixture
def lab_publications_csv():
    """Create a 'publications.csv' file"""
    lab_publication_content = [
        "project,publication",
        "ProjA,arXiv:1807.11104",
        "ProjA,arXiv:1807.11104v1",
    ]
    lab_publication_csv_path = pathlib.Path(
        "./tests/user_data/lab/\
                                             publications.csv"
    )
    write_csv(lab_publication_content, lab_publication_csv_path)

    yield lab_publication_content, lab_publication_csv_path
    lab_publication_csv_path.unlink()


@pytest.fixture
def lab_keywords_csv():
    """Create a 'keywords.csv' file"""
    lab_keyword_content = [
        "project,keyword",
        "ProjA,Study",
        "ProjA,Example",
        "ProjB,Alternate",
    ]
    lab_keyword_csv_path = pathlib.Path("./tests/user_data/lab/keywords.csv")
    write_csv(lab_keyword_content, lab_keyword_csv_path)

    yield lab_keyword_content, lab_keyword_csv_path
    lab_keyword_csv_path.unlink()


@pytest.fixture
def lab_protocol_csv():
    """Create a 'protocols.csv' file"""
    lab_protocol_content = [
        "protocol,protocol_type,protocol_description",
        "ProtA,IRB expedited review,Protocol for managing data ingestion",
        "ProtB,Alternative Method,Limited protocol for piloting only",
    ]
    lab_protocol_csv_path = pathlib.Path("./tests/user_data/lab/protocols.csv")
    write_csv(lab_protocol_content, lab_protocol_csv_path)

    yield lab_protocol_content, lab_protocol_csv_path
    lab_protocol_csv_path.unlink()


@pytest.fixture
def lab_user_csv():
    """Create a 'users.csv' file"""
    lab_user_content = [
        "lab,user,user_role,user_email,user_cellphone",
        "LabA,Sherlock,PI,Sherlock@BakerSt.com,+44 20 7946 0344",
        "LabA,Watson,Dr,DrWatson@BakerSt.com,+44 73 8389 1763",
        "LabB,Dr. Candace Pert,PI,Pert@gmail.com,+44 74 4046 5899",
        "LabA,User1,Lab Tech,fake@email.com,+44 1632 960103",
        "LabB,User2,Lab Tech,fake2@email.com,+44 1632 960102",
    ]
    lab_user_csv_path = pathlib.Path("./tests/user_data/lab/users.csv")
    write_csv(lab_user_content, lab_user_csv_path)

    yield lab_user_content, lab_user_csv_path
    lab_user_csv_path.unlink()


@pytest.fixture
def lab_source_csv():
    """Create a 'sources.csv' file"""
    sources_content = [
        "source,source_name,contact_details,source_description",
        "Provider1,Example Provider,+44 1632 960663 / Example@Provider.com,UK-based "
        + "supplier of lab subjects mus musculus",
    ]
    sources_csv_path = pathlib.Path("./tests/user_data/lab/sources.csv")
    write_csv(sources_content, sources_csv_path)

    yield sources_content, sources_csv_path
    sources_csv_path.unlink()


@pytest.fixture
def ingest_lab(
    pipeline,
    lab_csv,
    lab_project_csv,
    lab_publications_csv,
    lab_keywords_csv,
    lab_protocol_csv,
    lab_user_csv,
    lab_project_users_csv,
    lab_source_csv,
):
    """From workflow_session ingest.py, import ingest_lab, run"""
    from workflow_session.ingest import ingest_lab

    _, lab_csv_path = lab_csv
    _, lab_project_csv_path = lab_project_csv
    _, lab_publication_csv_path = lab_publications_csv
    _, lab_keyword_csv_path = lab_keywords_csv
    _, lab_protocol_csv_path = lab_protocol_csv
    _, lab_user_csv_path = lab_user_csv
    _, lab_project_user_csv_path = lab_project_users_csv
    _, lab_source_csv_path = lab_source_csv
    ingest_lab(
        lab_csv_path=lab_csv_path,
        project_csv_path=lab_project_csv_path,
        publication_csv_path=lab_publication_csv_path,
        keyword_csv_path=lab_keyword_csv_path,
        protocol_csv_path=lab_protocol_csv_path,
        users_csv_path=lab_user_csv_path,
        project_user_csv_path=lab_project_user_csv_path,
        sources_csv_path=lab_source_csv_path,
        verbose=verbose,
    )
    return


# Subject data and ingestion
@pytest.fixture
def subjects_csv():
    """Create a 'subjects.csv' file"""
    subject_content = [
        "subject,sex,subject_birth_date,subject_description,"
        + "death_date,cull_method",
        "subject5,F,2020-01-01 00:00:01,rich,2020-10-02 00:00:01,natural causes",
        "subject6,M,2020-01-01 00:00:01,manuel,2020-10-03 00:00:01,natural causes",
        "subjectX,F,2020-01-01 00:00:01,ally,2020-10-04 00:00:01,natural causes",
        "subjectY,M,2020-01-01 00:00:01,thom,2020-10-05 00:00:01,natural causes",
        "subjectZ,M,2020-01-01 00:00:01,winston,2020-10-06 00:00:01,natural causes",
    ]
    subject_csv_path = pathlib.Path("./tests/user_data/subject/subjects.csv")
    write_csv(subject_content, subject_csv_path)

    yield subject_content, subject_csv_path
    subject_csv_path.unlink()


@pytest.fixture
def subjects_part_csv():
    """Create a 'subjects_part.csv for Subject part tables"""
    subject_part_content = [
        "subject,protocol,user,line,strain,source,lab",
        "subject6,ProtA,User1,Black 6,B6,Provider1,LabA",
        "subject5,ProtA,User1,Brown 6,GP5.5,Provider1,LabA",
    ]
    subject_part_csv_path = pathlib.Path("./tests/user_data/subject/subjects_part.csv")
    write_csv(subject_part_content, subject_part_csv_path)

    yield subject_part_content, subject_part_csv_path
    subject_part_csv_path.unlink()


@pytest.fixture
def allele_csv():
    """Create a 'allele.csv' for pytests"""
    allele_content = [
        "allele,allele_standard_name,sequence,source,source_identifier,source_url",
        "Cdh23ahl,cadherin 23 (otocadherin); age related hearing loss 1,G-GT,Provider1,MGI:3028349,jax.org/strain/000664",
        "Apobec3Rfv3-r,apolipoprotein B mRNA editing enzyme - catalytic polypeptide 3; recovery from Friend virus 3,A-GT,Provider1,MGI:3028349,jax.org/strain/000665",
    ]
    allele_csv_path = pathlib.Path("./tests/user_data/subject/allele.csv")
    write_csv(allele_content, allele_csv_path)

    yield allele_content, allele_csv_path
    allele_csv_path.unlink()


@pytest.fixture
def cage_csv():
    """Create a 'cage.csv' for pytests"""
    cage_content = [
        "cage,subject,caging_datetime,user",
        "1,subject5,2020-01-02,User1",
        "2,subject6,2020-01-02,User2",
    ]
    cage_csv_path = pathlib.Path("./tests/user_data/subject/cage.csv")
    write_csv(cage_content, cage_csv_path)

    yield cage_content, cage_csv_path
    cage_csv_path.unlink()


@pytest.fixture
def breedingpair_csv():
    """Create a 'breedingpair.csv' for pytests"""
    breedingpair_content = [
        "subject,line,breeding_pair,bp_start_date,bp_end_date,father,mother,"
        + "litter_birth_date,num_of_pups,weaning_date,num_of_male,num_of_female",
        "subject5,Black 6,1/2,2019-10-15,2020-10-30,subject1,subject2,2020-10-20,2,"
        + "2020-10-30,1,1",
        "subject6,Black 6,1/2,2019-10-15,2020-10-30,subject1,subject2,2020-10-20,2,"
        + "2020-10-30,1,1",
        "subjectX,Brown 6,5/6,2019-12-31,2020-01-02,subject5,subject6,2020-01-01,3,"
        + "2020-01-02,2,1",
        "subjectY,Brown 6,5/6,2019-12-31,2020-01-02,subject5,subject6,2020-01-01,3,"
        + "2020-01-02,2,1",
        "subjectZ,Brown 6,5/6,2019-12-31,2020-01-02,subject5,subject6,2020-01-01,3,"
        + "2020-01-02,2,1",
    ]
    breedingpair_csv_path = pathlib.Path("./tests/user_data/subject/breedingpair.csv")
    write_csv(breedingpair_content, breedingpair_csv_path)

    yield breedingpair_content, breedingpair_csv_path
    breedingpair_csv_path.unlink()


@pytest.fixture
def genotype_test_csv():
    """Create a 'genotype_test.csv' for pytests"""
    genotype_test_content = [
        "subject,sequence,genotype_test_id,test_result",
        "subject5,G-GT,TestA,Present",
        "subject6,G-GT,TestA,Absent",
        "subject5,A-GT,TestA,Absent",
        "subject6,A-GT,TestA,Present",
    ]
    genotype_test_csv_path = pathlib.Path("./tests/user_data/subject/genotype_test.csv")
    write_csv(genotype_test_content, genotype_test_csv_path)

    yield genotype_test_content, genotype_test_csv_path
    genotype_test_csv_path.unlink()


@pytest.fixture
def line_csv():
    """Create a 'line.csv' for pytests"""
    line_content = [
        "line,species,is_active,allele",
        "Black 6,mus musculus,1,Cdh23ahl",
        "Black 6,mus musculus,1,Apobec3Rfv3-r",
        "Brown 6,mus musculus,1,Cdh23ahl",
    ]
    line_csv_path = pathlib.Path("./tests/user_data/subject/line.csv")
    write_csv(line_content, line_csv_path)

    yield line_content, line_csv_path
    line_csv_path.unlink()


@pytest.fixture
def strain_csv():
    """Create a 'strain.csv' for pytests"""
    strain_content = [
        "strain,strain_standard_name,strain_desc",
        "B6,C57BL/6J,First to have its genome sequenced",
        "GP5.5,Gcamp6-Thy,Expresses green fluorescent calcium indicator: GCaMP6f",
    ]
    strain_csv_path = pathlib.Path("./tests/user_data/subject/strain.csv")
    write_csv(strain_content, strain_csv_path)

    yield strain_content, strain_csv_path
    strain_csv_path.unlink()


@pytest.fixture
def zygosity_csv():
    """Create a 'zygosity.csv' for pytests"""
    zygosity_content = [
        "subject,allele,zygosity",
        "subject5,Cdh23ahl,Present",
        "subject5,Apobec3Rfv3-r,Heterozygous",
        "subject6,Cdh23ahl,Homozygous",
        "subjectX,Cdh23ahl,Present",
        "subjectY,Apobec3Rfv3-r,Present",
        "subjectZ,Apobec3Rfv3-r,Absent",
    ]
    zygosity_csv_path = pathlib.Path("./tests/user_data/subject/zygosity.csv")
    write_csv(zygosity_content, zygosity_csv_path)

    yield zygosity_content, zygosity_csv_path
    zygosity_csv_path.unlink()


@pytest.fixture
def ingest_subjects(
    pipeline,
    ingest_lab,
    lab_source_csv,
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
    """From workflow_session ingest.py, import ingest_subjects, run"""
    from workflow_session.ingest import ingest_subjects

    _, subject_csv_path = subjects_csv
    _, subject_part_csv_path = subjects_part_csv
    _, allele_csv_path = allele_csv
    _, cage_csv_path = cage_csv
    _, breedingpair_csv_path = breedingpair_csv
    _, genotype_test_csv_path = genotype_test_csv
    _, line_csv_path = line_csv
    _, strain_csv_path = strain_csv
    _, zygosity_csv_path = zygosity_csv
    ingest_subjects(
        subject_csv_path=subject_csv_path,
        subject_part_csv_path=subject_part_csv_path,
        allele_csv_path=allele_csv_path,
        cage_csv_path=cage_csv_path,
        breedingpair_csv_path=breedingpair_csv_path,
        genotype_test_csv_path=genotype_test_csv_path,
        line_csv_path=line_csv_path,
        strain_csv_path=strain_csv_path,
        zygosity_csv_path=zygosity_csv_path,
        verbose=verbose,
    )
    return


# Session data and ingestion
@pytest.fixture
def sessions_csv():
    """Create a 'sessions.csv' file"""
    session_csv_path = pathlib.Path("./tests/user_data/session/sessions.csv")
    session_content = [
        "subject,project,session_datetime,session_dir,session_note,user",
        "subject5,ProjA,2018-07-03 20:32:28,/subject5\\session1,"
        + "Successful data collection - no notes,User1",
        "subject6,ProjA,2021-06-02 14:04:22,/subject6/session1,"
        + "Ambient temp abnormally low,User2",
    ]
    write_csv(session_content, session_csv_path)

    yield session_content, session_csv_path
    session_csv_path.unlink()


@pytest.fixture
def ingest_sessions(ingest_lab, ingest_subjects, sessions_csv):
    """From workflow_session ingest.py, import ingest_sessions, run"""
    from workflow_session.ingest import ingest_sessions

    _, session_csv_path = sessions_csv
    ingest_sessions(session_csv_path=session_csv_path, verbose=verbose)
    return

import csv
from workflow_session.pipeline import lab, subject, session


def ingest_general(csvs, tables,
                   skip_duplicates=True, verbose=True):
    """
    Inserts data from a series of csvs into their corresponding table:
        e.g., ingest_general(['./lab_data.csv', './proj_data.csv'],
                                 [lab.Lab(),lab.Project()]
    ingest_general(csvs, tables, skip_duplicates=True)
        :param csvs: list of relative paths to CSV files.  CSV are delimited by commas.
        :param tables: list of datajoint tables with ()
        :param verbose: print number inserted (i.e., table length change)
    """
    for csv_filepath, table in zip(csvs, tables):
        with open(csv_filepath, newline='') as f:
            data = list(csv.DictReader(f, delimiter=','))
        if verbose:
            prev_len = len(table)
        table.insert(data, skip_duplicates=skip_duplicates,
                     # Ignore extra fields because some CSVs feed multiple tables
                     ignore_extra_fields=True)
        if verbose:
            insert_len = len(table) - prev_len     # report length change
            print(f'\n---- Inserting {insert_len} entry(s) '
                  + f'into {table.table_name} ----')



def ingest_lab(lab_csv_path='./user_data/lab/labs.csv',
               project_csv_path='./user_data/lab/projects.csv',
               publication_csv_path='./user_data/lab/publications.csv',
               keyword_csv_path='./user_data/lab/keywords.csv',
               protocol_csv_path='./user_data/lab/protocols.csv',
               users_csv_path='./user_data/lab/users.csv',
               project_user_csv_path='./user_data/lab/project_users.csv',
               skip_duplicates=True, verbose=True):
    """
    Inserts data from a CSVs into their corresponding lab schema tables.
    By default, uses data from workflow_session/user_data/lab/
    :param lab_csv_path:      relative path of lab csv
    :param project_csv_path:  relative path of project csv
    :param publication_csv_path:     relative path of publication csv
    :param keyword_csv_path:     relative path of keyword csv
    :param protocol_csv_path: relative path of protocol csv
    :param users_csv_path:    relative path of users csv
    :param project_user_csv_path: relative path of project users csv
    :param skip_duplicates=True: datajoint insert function param
    :param verbose: print number inserted (i.e., table length change)
    """

    # List with repeats for when mult dj.tables fed by same CSV
    csvs = [lab_csv_path, lab_csv_path,
            project_csv_path, project_csv_path,
            publication_csv_path, keyword_csv_path,
            protocol_csv_path, protocol_csv_path,
            users_csv_path, users_csv_path, users_csv_path,
            project_user_csv_path]
    tables = [lab.Lab(), lab.Location(),
              lab.Project(), lab.ProjectSourceCode(),
              lab.ProjectPublication(), lab.ProjectKeywords(),
              lab.ProtocolType(), lab.Protocol(),
              lab.UserRole(), lab.User(), lab.LabMembership(),
              lab.ProjectUser()]

    ingest_general(csvs, tables, skip_duplicates=skip_duplicates, verbose=verbose)


def ingest_subjects(subject_csv_path='./user_data/subject/subjects.csv',
                    subject_part_csv_path='./user_data/subject/subjects_part.csv',
                    skip_duplicates=True, verbose=True):
    """
    Inserts data from a subject csv into corresponding subject schema tables
    By default, uses data from workflow_session/user_data/subject/
    :param subject_csv_path:      relative path of csv for subject data
    :param subject_part_csv_path: relative path of csv for subject part tables
    :param skip_duplicates=True: datajoint insert function param
    :param verbose: print number inserted (i.e., table length change)
    """
    csvs = [subject_csv_path, subject_csv_path, subject_csv_path,
            subject_part_csv_path, subject_part_csv_path, subject_part_csv_path]
    tables = [subject.Subject(), subject.SubjectDeath(), subject.SubjectCullMethod(),
              subject.Subject.Protocol(), subject.Subject.User(), subject.Subject.Lab()]

    ingest_general(csvs, tables, skip_duplicates=skip_duplicates, verbose=verbose)



def ingest_sessions(session_csv_path='./user_data/session/sessions.csv',
                    skip_duplicates=True, verbose=True):
    """
    Inserts data from a sessions csv into corresponding session schema tables
    By default, uses data from workflow_session/user_data/session/
    :param session_csv_path:     relative path of session csv
    :param skip_duplicates=True: datajoint insert function param
    :param verbose: print number inserted (i.e., table length change)
    """
    csvs = [session_csv_path, session_csv_path, session_csv_path, session_csv_path,
            session_csv_path]
    tables = [session.Session(), session.SessionDirectory(),
              session.SessionNote(), session.ProjectSession(), 
              session.SessionExperimenter()]

    ingest_general(csvs, tables, skip_duplicates=skip_duplicates, verbose=verbose)


if __name__ == '__main__':
    ingest_lab()
    ingest_subjects()
    ingest_sessions()

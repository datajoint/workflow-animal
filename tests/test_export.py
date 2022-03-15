'''Test NWB export function
'''

__all__ = ['dj_config', 'pipeline', 'lab_csv', 'lab_project_csv', 'lab_user_csv',
           'lab_publications_csv', 'lab_keywords_csv', 'lab_protocol_csv',
           'lab_project_users_csv', 'ingest_lab', 'subjects_csv', 'subjects_part_csv',
           'ingest_subjects', 'sessions_csv', 'ingest_sessions']

from . import (dj_config, pipeline, lab_csv, lab_project_csv, lab_user_csv, 
               lab_publications_csv, lab_keywords_csv, lab_protocol_csv,
               lab_project_users_csv, ingest_lab, subjects_csv, subjects_part_csv, 
               ingest_subjects, sessions_csv, ingest_sessions)

def test_session_to_nwb(pipeline, lab_csv, lab_project_csv, lab_user_csv,
                         lab_publications_csv, lab_keywords_csv, lab_protocol_csv, 
                         lab_project_users_csv, ingest_lab, subjects_csv, 
                         subjects_part_csv, ingest_subjects, sessions_csv, 
                         ingest_sessions):
     import datetime
     from element_session.export.nwb import session_to_nwb

     nwbfile = session_to_nwb(session_key={"subject": "subject5",
                    "session_datetime": datetime.datetime(2018, 7, 3, 20, 32, 28),},
                              lab_key={"lab": "LabA"},
                              protocol_key={"protocol": "ProtA"},
                              project_key={"project": "ProjA"})

     assert nwbfile.session_id == "subject5_2018-07-03T20:32:28"
     assert nwbfile.session_description == "Successful data collection - no notes"
     assert nwbfile.session_start_time == datetime.datetime(2018, 7, 3, 20, 32, 28, 
                                                       tzinfo=datetime.timezone.utc)
     assert nwbfile.experimenter == ["User1"]

     assert nwbfile.subject.subject_id == "subject5"
     assert nwbfile.subject.sex == "F"

     assert nwbfile.institution == "Example Uni"
     assert nwbfile.lab == "The Example Lab"

     assert nwbfile.protocol == "ProtA"
     assert nwbfile.notes == "Protocol for managing data ingestion"

     assert nwbfile.experiment_description == "Example project to populate element-lab"
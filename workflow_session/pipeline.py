import datajoint as dj

from element_lab import lab
from element_animal import subject, genotyping
from element_session import session

from element_animal.subject import Subject
from element_animal.genotyping import Sequence, BreedingPair, Cage,\
                                      SubjectCaging, GenotypeTest
from element_lab.lab import Source, Lab, Protocol, User, Project, ProjectUser, \
                            ProjectKeywords, ProjectPublication, ProjectSourceCode
from element_session.session_with_datetime import Session, SessionDirectory, \
                                                  SessionExperimenter, SessionNote, \
                                                  ProjectSession

if 'custom' not in dj.config:
    dj.config['custom'] = {}

db_prefix = dj.config['custom'].get('database.prefix', '')

__all__ = ['genotyping', 'session', 'Subject', 'Source', 'Lab', 'Protocol', 'User',
           'Project', 'ProjectKeywords', 'ProjectPublication', 'ProjectSourceCode',
           'ProjectUser', 'Session', 'SessionDirectory', 'SessionExperimenter',
           'SessionNote', 'ProjectSession']

# Activate "lab", "subject", "session", "genotyping" schemas -------------

lab.activate(db_prefix + 'lab')

subject.activate(db_prefix + 'subject', linking_module=__name__)

from element_animal.export.nwb import subject_to_nwb
from element_lab.export.nwb import element_lab_to_nwb_dict

Experimenter = lab.User
session.activate(db_prefix + 'session', linking_module=__name__)

genotyping.activate(db_prefix + 'genotyping', db_prefix + 'subject', linking_module=__name__)

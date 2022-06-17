# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.7
#   kernelspec:
#     display_name: Python 3.8.11 ('ele')
#     language: python
#     name: python3
# ---

# # DataJoint U24 - Workflow Session

# This notebook will describe the steps to explore the lab and animal management tables created by the elements.
# Prior to using this notebook, please refer to the README for the installation instructions.

# change to the upper level folder to detect dj_local_conf.json
import os
if os.path.basename(os.getcwd())=='notebooks': os.chdir('..')
import datajoint as dj
dj.conn()

# Importing the module `workflow_session.pipeline` is sufficient to create tables inside the elements. This workflow comes prepackaged with example data and ingestion functions to populate lab, subject, and session tables.

from element_lab import lab
from element_animal import subject, genotyping
from element_session import session
from workflow_session.ingest import ingest_lab, ingest_subjects, ingest_sessions

ingest_lab(); ingest_subjects();ingest_sessions()

# ## Workflow architecture

lab.Lab()

dj.Diagram(lab)

lab.Source()

subject.Subject()

subject.Subject.Source()

dj.Diagram(subject)

genotyping.Litter()

dj.Diagram(genotyping)

session.Session()

dj.Diagram(session)

#

# +
# dj.Diagram(genotyping) + dj.Diagram(subject.Subject) + dj.Diagram(subject.Allele)
# -

# ## Explore each table

# check table definition with describe()
subject.Subject.describe()

# check table definition with dependencies with describe()
subject.Zygosity.describe()

# check the name of every attribute with heading, 
# which will spell out the foreign key definition inherited from another table
subject.Zygosity.heading

genotyping.BreedingPair.heading

# ## Insert data into Manual and Lookup tables

# Tables in this workflow are either manual tables or lookup tables. To insert into these tables, DataJoint provide method `.insert1()` and `insert()`.

subject.Subject.insert1(
    dict(subject='subject1', sex='M', subject_birth_date='2020-12-30', 
         subject_description='test animal'), skip_duplicates=True)
subject.Subject.insert1(
    ('subject2', 'F', '2020-11-30', 'test animal'), skip_duplicates=True)

# `skip_duplicates=True` will prevent an error if you already have data for the primary keys in a given entry.

subject.Subject()

# `insert()` takes a list of dicts or tuples
subject.Subject.insert(
    [dict(subject='subject3', sex='F', subject_birth_date='2020-12-30', 
            subject_description='test animal'),
     dict(subject='subject4', sex='M', subject_birth_date='2021-02-12', 
          subject_description='test animal')
    ],
    skip_duplicates=True)
subject.Subject.insert(
    [
        ('subject7', 'U', '2020-08-30', 'test animal'),
        ('subject8', 'F', '2020-09-30', 'test animal')
    ],
    skip_duplicates=True)

subject.Subject()

# For more documentation of insert, please refer to [DataJoint Docs](https://docs.datajoint.org/python/manipulation/1-Insert.html) and [DataJoint CodeBook](https://codebook.datajoint.io/)

# ## Insert into Manual and Lookup tables with a Graphical User Interface

# DataJoint also provides a graphical user interface ([DataJoint LabBook](https://github.com/datajoint/datajoint-labbook)) to support manual data insertions into DataJoint workflows. 
#
# ![DataJoint LabBook preview](https://github.com/datajoint/datajoint-labbook/blob/master/docs/sphinx/_static/images/walkthroughDemoOptimized.gif?raw=true)

#

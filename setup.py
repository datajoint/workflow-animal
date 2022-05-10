from setuptools import setup, find_packages
from os import path

pkg_name = "workflow_session"
here = path.abspath(path.dirname(__file__))

long_description = """"
# Workflow for lab, animal, and session management

Build a workflow for lab management and animal metadata using DataJoint Elements
+ [element-lab](https://github.com/datajoint/element-lab)
+ [element-animal](https://github.com/datajoint/element-animal)
+ [element-session](https://github.com/datajoint/element-session)
"""

with open(path.join(here, "requirements.txt")) as f:
    requirements = f.read().splitlines()

with open(path.join(here, pkg_name, "version.py")) as f:
    exec(f.read())

setup(
    name="workflow-session",
    version=__version__,
    description="DataJoint Elements for Lab, Animal and Session Management",
    long_description=long_description,
    author="DataJoint",
    author_email="info@datajoint.com",
    license="MIT",
    url="https://github.com/datajoint/workflow-session",
    keywords="neuroscience lab-management animal-management session datajoint",
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    install_requires=requirements,
)

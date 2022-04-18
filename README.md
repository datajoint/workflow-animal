# Workflow for lab, subject and session management

This directory provides an example workflow to save the information related to lab, subject, and session metadata data management, using the following datajoint elements
+ [element-lab](https://github.com/datajoint/element-lab)
+ [element-animal](https://github.com/datajoint/element-animal)
+ [element-session](https://github.com/datajoint/element-session)

This repository provides demonstrations for:
Setting up a workflow using different elements (see [pipeline.py](workflow_session/pipeline.py))

See the [Element Session documentation](https://elements.datajoint.org/description/session/) for the background information and development timeline.

For more information on the DataJoint Elements project, please visit https://elements.datajoint.org.  This work is supported by the National Institutes of Health.

## Workflow architecture
The lab and experiment subject management workflow presented here uses components from three DataJoint elements (element-lab, element-animal and element-session) assembled together into a functional workflow.

### element-lab

![element-lab](
https://github.com/datajoint/element-lab/raw/main/images/lab_diagram.svg)

### element-animal

![element-animal](
https://github.com/datajoint/element-animal/blob/main/images/subject_diagram.svg)

`genotyping` is designed for labs that handle animal care and genetic information themselves, which is optional.
![genotyping](https://github.com/datajoint/element-animal/blob/main/images/genotyping_diagram.svg)

### element-session
`session` is designed to handle metadata related to data collection, including collection date-time, file paths, and notes. Most workflows will include element-session as a starting point for further data entry.
![session](https://github.com/datajoint/element-session/blob/main/images/session_diagram.svg)

### This workflow
This workflow serves as an example of the upstream part of a typical data workflow, for examples using these elements in tandem with other data collection modalities, refer to:

+ [workflow-array-ephys](https://github.com/datajoint/workflow-array-ephys)
+ [workflow-calcium-imaging](https://github.com/datajoint/workflow-calcium-imaging)


## Installation instructions

+ The installation instructions can be found at the
[DataJoint Elements documentation](https://elements.datajoint.org/usage/install/).

## Interacting with the DataJoint workflow

+ Please refer to the following workflow-specific
[Jupyter notebooks](/notebooks) for an in-depth explanation of how to run the
workflow ([1-Explore_Workflow.ipynb](notebooks/1_Explore_Workflow.ipynb)).

## Citation

+ If your work uses DataJoint and DataJoint Elements, please cite the respective Research Resource Identifiers (RRIDs) and manuscripts.

+ DataJoint for Python or MATLAB
    + Yatsenko D, Reimer J, Ecker AS, Walker EY, Sinz F, Berens P, Hoenselaar A, Cotton RJ, Siapas AS, Tolias AS. DataJoint: managing big scientific data using MATLAB or Python. bioRxiv. 2015 Jan 1:031658. doi: https://doi.org/10.1101/031658

    + DataJoint ([RRID:SCR_014543](https://scicrunch.org/resolver/SCR_014543)) - DataJoint for `<Select Python or MATLAB>` (version `<Enter version number>`)

+ DataJoint Elements
    + Yatsenko D, Nguyen T, Shen S, Gunalan K, Turner CA, Guzman R, Sasaki M, Sitonic D, Reimer J, Walker EY, Tolias AS. DataJoint Elements: Data Workflows for Neurophysiology. bioRxiv. 2021 Jan 1. doi: https://doi.org/10.1101/2021.03.30.437358

    + DataJoint Elements ([RRID:SCR_021894](https://scicrunch.org/resolver/SCR_021894)) - Element Session (version `<Enter version number>`)
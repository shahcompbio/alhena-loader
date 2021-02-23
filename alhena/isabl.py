import os
import sys
import json
import logging
import collections
import math
import scipy.stats
import pandas as pd
import numpy as np
import alhena.constants as constants
import isabl_cli as ii
from scgenome.db.qc_from_files import get_qc_data_from_filenames


def get_scgenome_isabl_data(target_aliquot):

    
    APP_VERSION = '1.0.0'
    os.environ["ISABL_API_URL"] = 'https://isabl.shahlab.mskcc.org/api/v1/'
    os.environ['ISABL_CLIENT_ID'] = '1'
    VERSION = "0.0.1"
    
    experiment = ii.get_instances("experiments", aliquot_id=target_aliquot)[0]
        
    alignment = get_analyses('SCDNA-ALIGNMENT', VERSION, experiment.system_id)
    hmmcopy = get_analyses('SCDNA-HMMCOPY', VERSION, experiment.system_id)
    annotation = get_analyses('SCDNA-ANNOTATION', VERSION, experiment.system_id)

    #current = [alignment.pk, hmmcopy.pk, annotation.pk]

    #retrieve paths
    annotation_metrics = get_annotation_path(annotation.pk)
    hmmcopy_metrics,hmmcopy_reads,hmmcopy_segs = get_hmmcopy_path(hmmcopy.pk)
    alignment_metrics, gc_metrics = get_alignment_path(alignment.pk)

    results = get_qc_data_from_filenames(
        [annotation_metrics], [hmmcopy_reads], [hmmcopy_segs],
        [hmmcopy_metrics], [alignment_metrics], [gc_metrics]
    )

    hmmcopy_data = collections.defaultdict(list)

    for table_name, data in results.items():
        hmmcopy_data[table_name].append(data)
    for table_name in hmmcopy_data:
        hmmcopy_data[table_name] = pd.concat(
            hmmcopy_data[table_name], ignore_index=True)
    
    return hmmcopy_data

#get paths for scgenome get_qc_data_from_filenames
def get_analyses(app, version, exp_system_id):
    
    analyses = ii.get_instances(
        'analyses',
        application__name=app,
        application__version=version,
        targets__system_id=exp_system_id
    )
    assert len(analyses) == 1
    return analyses[0]


def get_alignment_path(pk):
    alignment_data = ii.Analysis(pk)
    alignment_metrics= alignment_data.results["alignment_metrics_csv"]
    gc_metrics = alignment_data.results["gc_metrics"]
    return alignment_metrics, gc_metrics

def get_hmmcopy_path(pk):
    hmmcopy_data = ii.Analysis(pk)
    hmmcopy_metrics = hmmcopy_data.results["hmmcopy_metrics_csv"]
    hmmcopy_reads = hmmcopy_data.results["reads"]
    hmmcopy_segs= hmmcopy_data.results["segments"]
    return hmmcopy_metrics,hmmcopy_reads,hmmcopy_segs

def get_annotation_path(pk):
    annotation_data = ii.Analysis(pk)
    annotation_metrics = annotation_data.results["metrics"]
    return annotation_metrics


def get_isabl_analysis_object(annotation_pk):
    APP_VERSION = '1.0.0'
    os.environ["ISABL_API_URL"] = 'https://isabl.shahlab.mskcc.org/api/v1/'
    os.environ['ISABL_CLIENT_ID'] = '1'
    VERSION = "0.0.1"
    
    #experiment = ii.get_instances("experiments", aliquot_id=target_aliquot)
    project = ii.get_instance("analyses",int(annotation_pk))

    experiment = project["targets"][0]
  
    record = {  
        "sample_id" : experiment["sample"]["identifier"],  
        "library_id" : experiment["library_id"],  
        "jira_id" : annotation_pk,  
        "description" : experiment["aliquot_id"]
        } 
    return record
    
def get_scgenome_isabl_annotation_pk(target_aliquot):
    APP_VERSION = '1.0.0'
    os.environ["ISABL_API_URL"] = 'https://isabl.shahlab.mskcc.org/api/v1/'
    os.environ['ISABL_CLIENT_ID'] = '1'
    VERSION = "0.0.1"
    
    experiment = ii.get_instances("experiments", aliquot_id=target_aliquot)[0]

    alignment = get_analyses('SCDNA-ALIGNMENT', VERSION, experiment.system_id)
    return alignment.pk

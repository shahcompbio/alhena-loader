
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


def get_scgenome_colossus_tantalus_data(directory):
    hmmcopy_data = collections.defaultdict(list)

    for table_name, data in load_qc_data(directory).items():
        hmmcopy_data[table_name].append(data)

    return hmmcopy_data
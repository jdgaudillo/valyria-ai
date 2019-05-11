import os
import glob
import time
import numpy as np

import pandas as pd

def readCSV(file, col_type, cols):

	if not cols:
		data = pd.read_csv(file, sep=",", dtype=col_type, encoding="utf-8")

	else:
		data = pd.read_csv(file, sep=",", dtype=col_type, encoding="utf-8", usecols=cols)	
	
	return data


def preprocessData(data):

	blob_directory = os.path.abspath("../Blob-Containers")

	data_dir = os.path.join(blob_directory, "processed/static")
	precincts_file = os.path.join(data_dir, "precincts.csv")
	col_type = {"VCM_ID": str}

	precincts = readCSV(precincts_file, col_type, ["VCM_ID", "REG_NAME", "PRV_NAME", "MUN_NAME", "REGISTERED_VOTERS"])

	data = data.merge(precincts, left_on = 'PRECINCT_CODE', right_on = 'VCM_ID', how = 'left')

	data = data.drop(data.loc[data.REG_NAME=="OAV"].index)

	return data


def logAnomalies(data, out_file):

	log_file = os.path.abspath("../Logs/" + out_file)

	with open(log_file, "a+") as f:
		data.to_csv(f, index=False, header=False)
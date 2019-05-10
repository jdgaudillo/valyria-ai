import warnings
warnings.filterwarnings("ignore")

import os
import glob
import time
import numpy as np

import pandas as pd 


def readCSV(file):
	data = pd.read_csv(file, sep=",", dtype={"PRECINCT_CODE": str})
	
	return data


def checkForDuplication(data, cols):
	duplicates = data[data.duplicated(cols, keep=False)]

	duplicates = duplicates.groupby(["RECEPTION_DATE", "PRECINCT_CODE"]).first()
	duplicates = duplicates.reset_index()

	return duplicates
	

def logDuplicates(duplicates):
	log_file = os.path.abspath("../Logs/duplicate_log.csv")

	with open(log_file, "a+") as f:
		duplicates.to_csv(f, index=False)


print("\n============RUNNING DUPLICATE SCRIPT===================\n")

start_time = time.time()

blob_directory = os.path.abspath("../Blob-Containers")
data_dir = os.path.join(blob_directory, "unprocessed")

results_file = os.path.join(data_dir, "results.csv")
static_cols = ["PRECINCT_CODE", "CONTEST_CODE", "CANDIDATE_NAME", "PARTY_CODE"]

results = readCSV(results_file)

duplicates = checkForDuplication(results, static_cols)

if not len(duplicates):
	print("No double transmission")
	print("TOTAL RUNTIME:", time.time() - start_time)
	print("\n============END OF DUPLICATE SCRIPT================\n")

else:
	print("Flagged double transmission!")

	print("Number of duplicates found: {}".format(len(duplicates)/2), "\n")
	print(duplicates)

	if len(duplicates)%2 != 0:
		print("\n*********NOTES!***********")
		print("The detected double transmission has the same RECEPTION_DATE.\n")

	logDuplicates(duplicates)

	print("TOTAL RUNTIME: ", time.time() - start_time, "")
	print("\n================END OF DUPLICATE SCRIPT================\n")
	
	
	
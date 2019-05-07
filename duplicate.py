import warnings
warnings.filterwarnings("ignore")

import os
import glob
import numpy as np

import pandas as pd 


def readCSV(file):
	data = pd.read_csv(file, sep=",")
	
	return data


def checkForDuplication(data, cols):
	duplicates = data[data.duplicated(static_cols)]

	return duplicates
	

def logDuplicates(duplicates, order):
	duplicates.to_csv("duplicate-logs/log_"+str(order)+".csv", index=False)


def renameAndSave(results, results_file, order):
	filename = results_file.split(".")[0]
	filename = filename + "_" + str(order-1) + ".csv"
	
	results.to_csv(filename, index=False)


# CHANGE DIRECTORIES FOR DEPLOYMENT
unprocessed_files = glob.glob("unprocessed/*.csv")
order = len(unprocessed_files)

results_file = "unprocessed/results_0.csv"
static_cols = ["PRECINCT_CODE", "CONTEST_CODE", "CANDIDATE_NAME", "PARTY_CODE"]

results = readCSV(results_file)

duplicates = checkForDuplication(results, static_cols)

if not len(duplicates):
	duplicate_flag = 0
else:
	duplicate_flag = 1
	print(duplicates)
	logDuplicates(duplicates, flag, order)

renameAndSave(results, results_file, order)







import os
import glob
import time

import pandas as pd
import numpy as np 


def findOldFile(files):
	order = [int(file.split("_")[1].split(".")[0]) 
			for file in files if "_" in file]

	max_index = order.index(max(order))
	
	return(files[max_index])


def readCSV(file, col_type, cols):
	
	data = pd.read_csv(file, sep=",", dtype=col_type, encoding="utf-8", usecols=cols)
	
	return data


def preprocessData(data):
	data_dir = os.path.join(blob_directory, "processed/static")
	precincts_file = os.path.join(data_dir, "precincts.csv")
	col_type = {"VCM_ID": str}

	precincts = readCSV(precincts_file, col_type, ["VCM_ID", "REG_NAME", "PRV_NAME", "MUN_NAME", "REGISTERED_VOTERS"])

	data = data.merge(precincts, left_on = 'PRECINCT_CODE', right_on = 'VCM_ID', how = 'left')

	data = data.drop(data.loc[data.REG_NAME=="OAV"].index)

	return data


def addVoterTurnout(data):

	merged_data = preprocessData(data)

	merged_data = merged_data.drop_duplicates(subset=["PRECINCT_CODE"])
	
	merged_data.loc[:, "PERCENT_VOTER_TURNOUT"] = (merged_data.NUMBER_VOTERS / merged_data.REGISTERED_VOTERS) * 100.

	merged_data = merged_data.drop(["VCM_ID", "PRECINCT_CODE"], axis=1)

	mun_group = merged_data.groupby(["REG_NAME", "PRV_NAME", "MUN_NAME"], sort=False)["PERCENT_VOTER_TURNOUT"].mean().to_frame().reset_index()
	
	return mun_group


def logVoterTurnout(data):
	log_file = os.path.abspath("../Logs/voter_turnout_log.csv")

	with open(log_file, "a+") as f:
		data.to_csv(f, index=False, header=False)


if __name__ == '__main__':

	print("\n============RUNNING VOTER TURNOUT LIVE===================\n")

	start_time = time.time()

	blob_directory = os.path.abspath("../Blob-Containers")
	data_dir = os.path.join(blob_directory, "unprocessed")

	cols = ["PRECINCT_CODE", "NUMBER_VOTERS", "RECEPTION_DATE"]
	col_type = {"PRECINCT_CODE": str}

	results_file = os.path.join(data_dir, "results.csv")

	unprocessed_files = glob.glob(data_dir + "/*_*.csv")
	order = len(unprocessed_files)

	if order != 0:
		old_file = findOldFile(unprocessed_files)

		old_results = readCSV(old_file, col_type, cols)
		old_shape = old_results.shape

	else:
		old_shape = [0, 10]

	results = readCSV(results_file, col_type, cols)

	voter_turnout = addVoterTurnout(results.loc[old_shape[0]:])
			
	logVoterTurnout(voter_turnout)

	print("Updated voter_turnout_log.csv file.")

	print("\nTOTAL RUNTIME: ", time.time() - start_time)

	print("\n============END OF VOTER TURNOUT LIVE===================\n")



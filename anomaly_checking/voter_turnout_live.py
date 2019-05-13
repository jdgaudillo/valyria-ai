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
	precincts_file = os.path.join(data_dir, "reg_vote.csv")
	col_type = {"VCM_ID": str}

	precincts = readCSV(precincts_file, col_type, ["VCM_ID", "REG_NAME", "PRV_NAME", "MUN_NAME", "REGISTERED_VOTERS", "total_regvoters"])

	#precincts.loc[:, "total_regvoters"] = precincts.REGISTERED_VOTERS.sum()

	data = data.drop_duplicates(subset=["PRECINCT_CODE"])

	data = data.merge(precincts, left_on = 'PRECINCT_CODE', right_on = 'VCM_ID', how = 'left')

	data = data.drop(data.loc[data.REG_NAME=="OAV"].index)

	return data


def addVoterTurnout(df):

	voter_turnout = df.groupby(["REG_NAME", "PRV_NAME", "MUN_NAME", "total_regvoters"], sort=False)["NUMBER_VOTERS"].sum().reset_index()
	
	voter_turnout.loc[:, "PERCENT_VOTER_TURNOUT"] = (voter_turnout.NUMBER_VOTERS / voter_turnout.total_regvoters) * 100.

	return voter_turnout


def addTransmittedPrecincts(df):

	data_dir = os.path.join(blob_directory, "processed/static")
	total_precincts_file = os.path.join(data_dir, "total_precincts.csv")
	
	total_precincts = pd.read_csv(total_precincts_file, sep=",", encoding="utf-8")
	total_precincts = total_precincts.set_index("try_key")

	df.loc[:, "NUMBER_TRANSMITTED_PRECINCTS"] = 1.
	
	transmitted_precincts = df.groupby(["REG_NAME", "PRV_NAME", "MUN_NAME"], sort=False)["NUMBER_TRANSMITTED_PRECINCTS"].sum().to_frame().reset_index()
	transmitted_precincts.loc[:, "try_key"] = transmitted_precincts.PRV_NAME + "-" + transmitted_precincts.MUN_NAME
	transmitted_precincts = transmitted_precincts.set_index("try_key")

	res = pd.concat([transmitted_precincts, total_precincts], axis=1, join_axes=[transmitted_precincts.index])

	res.columns = ['REG_NAME', 'PRV_NAME', 'MUN_NAME', 'NUMBER_TRANSMITTED_PRECINCTS', 
					'REG_NAME_x', 'PRV_NAME_x', 'MUN_NAME_x', 'TOTAL_PRECINCTS']
			
	res = res.drop(["REG_NAME", "PRV_NAME", "MUN_NAME", "REG_NAME_x", "PRV_NAME_x", "MUN_NAME_x"], axis=1)

	res.loc[:, "PERCENT_TRANSMITTED_PRECINCTS"] = (res.NUMBER_TRANSMITTED_PRECINCTS/res.TOTAL_PRECINCTS) * 100.

	return res


def addColumns(data):

	voter_turnout = addVoterTurnout(data)
	voter_turnout.loc[:, "try_key"] = voter_turnout.PRV_NAME + "-" + voter_turnout.MUN_NAME
	voter_turnout = voter_turnout.set_index("try_key")

	transmitted_precincts = addTransmittedPrecincts(data)
	
	res = pd.concat([voter_turnout, transmitted_precincts], axis=1, join_axes=[voter_turnout.index])
	res = res.reset_index(drop=True)

	return res


def logVoterTurnout(data, order):
	
	log_file = os.path.abspath("../Logs/Voter-Turnout-Live/voter_turnout_" + str(order) + ".csv")

	with open(log_file, "w+", encoding="utf-8") as f:
		data.to_csv(f, index=False, encoding="utf-8")

	print("\nSuccessfully added log ", log_file.split("/")[-1])


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

	results = readCSV(results_file, col_type, cols)

	merged_results = preprocessData(results)

	final_df = addColumns(merged_results)

	print("Printing first 5 rows in the log file. \n")
	print(final_df.head())
			
	logVoterTurnout(final_df, order)

	print("\nTOTAL RUNTIME: ", time.time() - start_time)

	print("\n============END OF VOTER TURNOUT LIVE===================\n")
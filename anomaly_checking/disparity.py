import warnings
warnings.filterwarnings("ignore")

import os
import sys
import glob
import time
import numpy as np

import pandas as pd 

from utils import readCSV

def findOldFile(files):
	order = [int(file.split("_")[1].split(".")[0]) 
			for file in files if "_" in file]

	max_index = order.index(max(order))
	
	return(files[max_index])


def compareDataframes(data_old, data_new, shape_old):

	return not data_old.equals(data_new[:shape_old[0]])


def getAnomalousRow(data_old, data_new):
	compared = (data_old != data_new[:shape_old[0]])

	cols = compared.columns

	subsetter = []

	for row in compared.iterrows():
		true_col = []

		index, value = row

		if value.any():
			subsetter.append(index)

	return subsetter


def logDisparity(disparity):
	log_file = os.path.abspath("../Logs/disparity_log.csv")

	with open(log_file, "a+") as f:
		disparity.to_csv(f, index=False, header=False)
		

if __name__ == '__main__':
	print("\n============RUNNING DISPARITY SCRIPT===================\n")

	start_time = time.time()

	blob_directory = os.path.abspath("../Blob-Containers")
	data_dir = os.path.join(blob_directory, "unprocessed")

	unprocessed_files = glob.glob(data_dir + "/*_*.csv")
	order = len(unprocessed_files)

	if order == 0:
		print("First transmission. No disparity.")
		print("\n============END OF DISPARITY SCRIPT===================\n")
		sys.exit()

	old_file = findOldFile(unprocessed_files)
	new_file = os.path.join(data_dir, "results.csv")
	
	col_types = {"PRECINCT_CODE": str, "VCM_ID": str}
	cols = []

	results_old = readCSV(old_file, col_types, cols)
	results_new = readCSV(new_file, col_types, cols)

	shape_old = results_old.shape
	shape_new = results_new.shape

	disparity_flag = compareDataframes(results_old, results_new, shape_old)

	if disparity_flag:
		print("There's a disparity in the transmitted files! \n")
		anomalous_index = getAnomalousRow(results_old, results_new)
				
		disparity_old = results_old.iloc[anomalous_index]
		disparity_old.loc[:, "DISPARITY_INDEX"] = str(order)

		disparity_new = results_new.iloc[anomalous_index]
		disparity_new.loc[:, "DISPARITY_INDEX"] = str(order)

		print(disparity_old)
		print(disparity_new)

		logDisparity(disparity_old)
		logDisparity(disparity_new)

		print("\nTOTAL RUNTIME: ", time.time() - start_time)

		print("\n============END OF DISPARITY SCRIPT===================\n")

	else:
		print("The old and new result files have no disparity")

		print("\nTOTAL RUNTIME: ", time.time() - start_time)

		print("\n============END OF DISPARITY SCRIPT===================\n")


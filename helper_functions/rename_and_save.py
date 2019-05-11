import os
import glob

blob_directory = os.path.abspath("../Blob-Containers")
data_dir = os.path.join(blob_directory, "unprocessed")
source = os.path.join(data_dir, "results.csv")

unprocessed_files = glob.glob(data_dir + "/*_*.csv")
order = len(unprocessed_files)


def renameAndSave(source, order):
	dest = ''.join(map(str, source.split(".")[:-1])) + "_" + str(order) + ".csv"
	os.rename(source, dest)


renameAndSave(source, order)

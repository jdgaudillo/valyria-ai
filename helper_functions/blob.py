import io
import os
import time
import shutil
import pandas as pd

from zipfile import ZipFile
from azure.storage.blob import PublicAccess
from azure.storage.blob import BlockBlobService


def downloadBlob(account_name, account_key, container, data_dir, blob_name):
	block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

	print("Downloading {}".format(blob_name))

	downloaded_file = os.path.join(data_dir, blob_name)
	block_blob_service.get_blob_to_path(container, blob_name, downloaded_file)

	print("Successfully downloaded ", downloaded_file.split("/")[-1])
	print("\n")

	return downloaded_file


def uploadBlob(account_name, account_key, container, local_paths):
	block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
	
	if type(local_paths) == str:
		blob_files = local_paths.split("/")[-1]
		blob_paths = os.path.join("dynamic", blob_files)
		print(blob_paths)
		print("Uploading {}".format(blob_paths))
		block_blob_service.create_blob_from_path(container, blob_paths, local_paths)

	elif type(local_paths) == list:
		blob_files = [local_path.split("/")[-1] for local_path in local_paths]
		blob_paths = [os.path.join("dynamic", blob_file) for blob_file in blob_files]

		for i, blob_path in enumerate(blob_paths):
			print("Uploading {}".format(blob_path))
			block_blob_service.create_blob_from_path(container, blob_path, local_paths[i])


def copyBlob(source, dest):
	shutil.copy(source, dest)


def extractBlob(source, dest):
	with ZipFile(source) as zipObj:
		filenames = zipObj.namelist()

		extract_file = source.split("/")[-1][:-4] + "/results.csv"

		print("Extracting file.................")
		zipObj.extract(extract_file, dest)
		print("Successfully extracted file!")
		print("==============EXTRACT BLOB====================\n")

	extract_file = os.path.join(dest, extract_file)
	dest = os.path.abspath("../Blob-Containers/unprocessed")
	copyBlob(extract_file, dest)
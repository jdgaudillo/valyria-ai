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

	return downloaded_file


def uploadBlob(account_name, account_key, container, local_paths):
	block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)

	blob_files = [local_path.split("/")[-1] for local_path in local_paths]
	blob_paths = [os.path.join("dynamic", blob_file) for blob_file in blob_files]

	for i, blob_path in enumerate(blob_paths):
		print("Uploading {}".format(blob_path))
		block_blob_service.create_blob_from_path(container, blob_path, local_paths[i])


def extractBlob(source, dest):
	with ZipFile(source) as zipObj:
		filenames = zipObj.namelist()

		extract_file = source.split("/")[-1][:-4] + "/results.csv"
		print(extract_file)

		print("Extracting file.................")
		zipObj.extract(extract_file, dest)
		print("Successfully extracted file!")
		print("==============EXTRACT BLOB====================")

		"""for name in filenames:
									if name.endswith(".csv"):
										print("Extracting file.....")
						
										zipObj.extract(name, dest)
						
										print("Successfully extracted ", name.split("/")[-1], "to ", dest)"""

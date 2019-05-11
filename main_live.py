import sys

import os
import glob
import numpy as np

import watchdog
import pandas as pd

from helper_functions.blob import downloadBlob, extractBlob, uploadBlob
from helper_functions.watcher import *


blob_name = "precinct_results_2019.01.19.15.39.zip"

account_name = "halalanaidata"
account_key = "BglYcnBSLXLgXKEeY4dGu9bsYw7QKBeuJnAWD4EsGAXaUi+7uQn+Ltf8Hf+DEOj/OCUPIPcAWkEV0gtrqILQcQ=="
container = "dump"

root = os.getcwd()
blob_directory = os.path.abspath("../Blob-Containers")

dump_dir = os.path.join(blob_directory, "dump")

downloaded_file = downloadBlob(account_name, account_key, container, dump_dir, blob_name)

extractBlob(downloaded_file, dump_dir)




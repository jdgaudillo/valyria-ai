import pandas as pd
import numpy as np
import time
import datetime
import re
import os

static_dir = os.path.abspath('../processed/static')
unprocessed_dir = os.path.abspath('../unprocessed')
dynamic_dir = os.path.abspath('../processed/dynamic')


def readFile(filename):
	df = pd.read_csv(filename, encoding = 'utf-8', dtype = str)
	return df

def count_prec(df, col):
	df.drop_duplicates(['PRECINCT_CODE'], keep = 'first', inplace = True)
	grouped = df.groupby(col)
	keys = grouped.groups.keys()
	df_2 = pd.DataFrame(index = keys, columns = ['COUNT_TRANSMITTED_PREC'])
	for key in keys:
		a = grouped.get_group(key)
		df_2.loc[key, 'COUNT_TRANSMITTED_PREC'] = a.shape[0]
	df_2.index.name = col
	return df_2

def transform(df, id_vars, value_vars, outfile = False):
	transformed = df.melt(id_vars = id_vars, value_vars=value_vars)
	if outfile:
		save_file(transformed, outfile)
	return transformed

def summarize_candidate(df, col):
	df = df.loc[df.CONTEST_NAME == 'SENATOR PHILIPPINES']
	s = time.time()
	summarized_candidate = pd.DataFrame(index = df[col].unique().tolist(), columns = df.CANDIDATE_NAME.unique().tolist())
	grouped = df.groupby([col, 'CANDIDATE_NAME'])
	keys = grouped.groups.keys()
	for key in keys:
	    a = grouped.get_group(key)
	    summarized_candidate.loc[key[0]][key[1]] = a.VOTES_AMOUNT.astype(float).sum()
	e = time.time()
	summarized_candidate.index.name = col
	summarized_candidate.reset_index(inplace = True)
	summarized_candidate = transform(summarized_candidate, ['PRV_NAME'], df.CANDIDATE_NAME.unique())
	return summarized_candidate

def aggregatePrecincts(df, col):
	grouped = df.groupby(col)
	keys = grouped.groups.keys()
	df_2 = pd.DataFrame(index = keys, columns = ['TOTAL_PRECINCTS', 'REG_NAME'])
	for key in keys:
		a = grouped.get_group(key)
		df_2.loc[key, 'TOTAL_PRECINCTS'] = a.TOTAL_PRECINCTS.astype(float).sum()
		df_2.loc[key, 'REG_NAME'] = a.iloc[0]['REG_NAME']
	df_2.index.name = col
	return df_2.reset_index()

def saveFile(df, filename):
	df.to_csv(filename, encoding = 'utf-8')

def prep_results():
	results = readFile(os.path.join(unprocessed_dir, 'results_orig.csv'))
	contests = pd.read_csv(os.path.join(static_dir, 'contests.csv'), encoding = 'utf-8', dtype = {'CONTEST_CODE':str})
	results = results.merge(contests, on = 'CONTEST_CODE', how = 'left')
	precincts = readFile(os.path.join(static_dir, 'precincts.csv'))
	results = results.merge(precincts, left_on = 'PRECINCT_CODE', right_on = 'VCM_ID', how = 'left')
	total_prec = readFile(os.path.join(static_dir, 'total_precincts_prv.csv'))
	return results, total_prec

def transmitted(time_stamp):
	results, total_prec = prep_results()
	counted_prec = count_prec(results, 'PRV_NAME')
	counted_prec = counted_prec.merge(total_prec, on = 'PRV_NAME', how = 'left')
	counted_prec['TOTAL_PRECINCTS'] = counted_prec['TOTAL_PRECINCTS'].astype(float)
	counted_prec['PERCENT_TRANSMITTED'] = ((counted_prec['COUNT_TRANSMITTED_PREC']/counted_prec['TOTAL_PRECINCTS']*1.0)*100).astype(float)
	counted_prec['TRANSMITTED_80'] = counted_prec['PERCENT_TRANSMITTED'].apply(lambda x: 1 if (x >= 80.0) else 0)
	counted_prec['TRANSMITTED_60'] = counted_prec['PERCENT_TRANSMITTED'].apply(lambda x: 1 if (x >= 60.0) else 0)
	counted_prec['TRANSMITTED_50'] = counted_prec['PERCENT_TRANSMITTED'].apply(lambda x: 1 if (x >= 50.0) else 0)

	reg_candidate_summary = summarize_candidate(results, 'PRV_NAME')
	transmit_80 = counted_prec.loc[counted_prec['TRANSMITTED_80'] == 1].nlargest(10, 'PERCENT_TRANSMITTED')
	transmit_80 = transmit_80.merge(reg_candidate_summary, on = 'PRV_NAME', how = 'left')
	transmit_80['value'] = transmit_80['value'].astype(float)
	grouped = transmit_80.groupby('PRV_NAME')
	keys = grouped.groups.keys()
	df = []
	for key in keys:
		a = grouped.get_group(key)
		a = a.nlargest(12, 'value')
		df.append(a)
	df = pd.concat(df)
	df['timestamp'] = time_stamp 
	saveFile(df, os.path.join(dynamic_dir, 'transmitted_top12.csv'))

if __name__ == '__main__':
	time_stamp = input('Enter time_stamp of COMELEC results: ')
	transmitted(time_stamp)




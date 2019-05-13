import pandas as pd
import numpy as np
import warnings
import time
import os
import re

warnings.filterwarnings('ignore')

def initialize(unprocessed_dir, processed_dir):
	contest = pd.read_csv(os.path.join(processed_dir, 'static/contests.csv'), encoding = 'utf-8')
	positions = ['SENATOR', 'GOVERNOR', 'VICE-GOVERNOR', 'MAYOR', 'VICE-MAYOR','PARTY LIST']
	codes = []
	for pos in positions:
	    mask = contest['CONTEST_NAME'].str.contains(pos)
	    codes_1 = contest.loc[mask]
	    codes.append(codes_1)
	codes = pd.concat(codes)

	candidates = pd.read_csv(os.path.join(processed_dir, 'static/candidates.csv'), encoding = 'utf-8')
	candidates = candidates.loc[candidates['CONTEST_CODE'].isin(codes['CONTEST_CODE'].values)]
	precincts = pd.read_csv(os.path.join(processed_dir, 'static/precincts.csv'), encoding = 'utf-8', dtype = {'VCM_ID':str})
	reg_voters = pd.read_csv(os.path.join(processed_dir, 'static/registered_voters.csv'), encoding = 'utf-8')
	reg_voters['PRV_MUN'] = reg_voters['PRV_NAME']+'-'+reg_voters['MUN_NAME']

	return codes, candidates, precincts, reg_voters

def prep_results(unprocessed_dir, precincts):
	print('Prepping results.csv')
	s = time.time()
	df = pd.read_csv(os.path.join(unprocessed_dir,'results_orig.csv'), encoding = 'utf-8', dtype = {'PRECINCT_CODE':str})
	df = df.merge(precincts, left_on = 'PRECINCT_CODE', right_on = 'VCM_ID', how = 'left')
	df = df.loc[df['REG_NAME'] != 'OAV']
	df['PRV_MUN'] = df['PRV_NAME'] + '-' + df['MUN_NAME']
	df['TURNOUT'] = df['NUMBER_VOTERS'].astype(float)/df['REGISTERED_VOTERS'].astype(float)
	df.drop_duplicates(['PRECINCT_CODE'], inplace = True, keep = 'first')
	e = time.time()
	print('Total Duration of Prepping: ', (e-s)/60,'m and', (e-s)%60,'s')
	
	return df

def summarize_candidate(df, col, candidates):
	print('Summarizing...')
	s = time.time()
	summarized_candidate = pd.DataFrame(index = df[col].unique().tolist(), columns = candidates)
	grouped = df.groupby([col, 'CANDIDATE_NAME'])
	keys = grouped.groups.keys()
	for key in keys:
	    a = grouped.get_group(key)
	    summarized_candidate.loc[key[0]][key[1]] = a.VOTES_AMOUNT.sum()
	summarized_candidate.index.name = col
	summarized_candidate.reset_index(inplace = True)
	# if col == 'PRV_MUN':
	# 	ncr_manila = summarized_candidate[summarized_candidate['PRV_MUN'].str.contains('NATIONAL CAPTIAL REGION - MANILA')]
	# 	summarized_candidate = summarized_candidate[~summarized_candidate['PRV_MUN'].str.contains('NATIONAL CAPTIAL REGION - MANILA') == False]
	# 	summarized_manila = {}
	# 	summarized_manila['PRV_MUN'] = 'NATIONAL CAPITAL REGION - MANILA - MANILA'
	# 	for candidate in candidates:
	# 		summarized_manila[candidate] = ncr_manila[candidate].sum()
	# 	summarized_manila = pd.DataFrame(summarized_manila, index =[0])
	# 	summarized_candidate = pd.concat([summarized_candidate, summarized_manila], axis = 0)
	e = time.time()
	print('Total Duration of Summarization: ', (e-s)/60,'m and', (e-s)%60,'s')
	

	return summarized_candidate

def summarize(df, col, cols):
	summarized = pd.DataFrame(index = df[col].unique().tolist(), columns = cols)
	grouped = df.groupby(col)
	keys = grouped.groups.keys()
	for key in keys:
		a = grouped.get_group(key)
		a.drop_duplicates(subset = ['PRECINCT_CODE'], keep = 'first', inplace = True)
		for i in cols:
			if i == 'TURNOUT':
				summarized.loc[key]['TURNOUT'] = (a[i].sum()/(a.shape[0]*1.0))*100
			summarized.loc[key][i] = a[i].sum()
	summarized.index.name = col
	return summarized.reset_index()

def save_file(df, outfile):
	print('Saving file {}'.format(outfile))
	df.to_csv(outfile, encoding = 'utf-8', index = False)

def add_info(df, level, candidates, codes, outfile = False):
	df['LEVEL'] = level
	df = df.merge(candidates, left_on = 'variable', right_on = 'CANDIDATE_NAME', how = 'left')
	df = df.merge(codes, left_on = 'CONTEST_CODE', right_on = 'CONTEST_CODE', how = 'left')
	df['VICE_MAYOR'] = df['CONTEST_NAME'].apply(lambda x: 'VICE-MAYOR' if re.search(r'VICE-MAYOR', x) else 'NOPE')
	df['CONTEST'] = df['CONTEST_NAME'].apply(lambda x: 'PROVINCIAL GOVERNOR' if re.search(r'PROVINCIAL GOVERNOR', x) else x)
	df['CONTEST'] = df['CONTEST'].apply(lambda x: 'PROVINCIAL VICE-GOVERNOR' if re.search(r'PROVINCIAL VICE-GOVERNOR', x) else x)
	df['CONTEST'] = df['CONTEST'].apply(lambda x: 'MAYOR' if re.search(r'MAYOR', x) else x)
	df['CONTEST'] = df['CONTEST'].apply(lambda x: 'SENATOR PHILIPPINES' if re.search(r'SENATOR PHILIPPINES', x) else x)
	df = df.dropna(subset = ['value'], axis = 0)
	if outfile:
		save_file(df, outfile)
	return df

def transform(df, id_vars, value_vars, outfile = False):
	transformed = df.melt(id_vars = id_vars, value_vars=value_vars)
	if outfile:
		save_file(transformed, outfile)
	return transformed

def vote_type(col, level, reg_voters, results):
	df = summarize(results, col, ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE', 'TURNOUT', 'REGISTERED_VOTERS'])
	# df['REGISTERED_VOTERS'] = df[[col]].merge(reg_voters, how = 'left').REGISTERED_VOTERS
	# df['TURNOUT'] = (df['NUMBER_VOTERS']/df['REGISTERED_VOTERS'])*100
	# df = transform(df, col, ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE', 'REGISTERED_VOTERS', 'TURNOUT'])
	df['LEVEL'] = level
	return df

def candidate_vote(results, col, level, candidates, codes, outfile = False):
	df = summarize_candidate(results, col, candidates.CANDIDATE_NAME.unique())
	df = transform(df, id_vars = col, value_vars = candidates.CANDIDATE_NAME.unique())
	df = add_info(df, level, candidates, codes, outfile)
	return df

def preprocess():
	s = time.time()
	unprocessed_dir = os.path.abspath('../unprocessed')
	processed_dir = os.path.abspath('../processed')

	codes, candidates, precincts, reg_voters = initialize(unprocessed_dir, processed_dir)
	results = prep_results(unprocessed_dir, precincts)
	

	vote_reg = vote_type('REG_NAME', 'REGION', reg_voters, results)
	vote_prv = vote_type('PRV_NAME', 'PROVINCE', reg_voters, results)
	vote_mun = vote_type('PRV_MUN', 'MUNICIPALITY', reg_voters, results)
	vote_mun['PRV_NAME'] = vote_mun['PRV_MUN'].apply(lambda x:re.search(r'(.*)-', x).group(1))
	vote_mun['MUN_NAME'] = vote_mun['PRV_MUN'].apply(lambda x:re.search(r'.*-(.*)', x).group(1))
	vote_summary = pd.concat([vote_reg,vote_prv,vote_mun], axis = 0)
	save_file(vote_summary, os.path.join(processed_dir,'dynamic/vote_type_summary.csv'))


	reg_summary = candidate_vote(results, 'REG_NAME', 'REGION', candidates, codes, outfile = os.path.join(processed_dir, 'dynamic/vote_count_reg.csv'))
	prv_summary = candidate_vote(results, 'PRV_NAME', 'PROVINCE', candidates, codes, outfile = os.path.join(processed_dir, 'dynamic/vote_count_prv.csv'))
	mun_summary = candidate_vote(results, 'PRV_MUN', 'MUNICIPALITY', candidates, codes)
	mun_summary['PRV_NAME'] = mun_summary['PRV_MUN'].apply(lambda x:re.search(r'(.*)-', x).group(1))
	mun_summary['MUN_NAME'] = mun_summary['PRV_MUN'].apply(lambda x:re.search(r'.*-(.*)', x).group(1))
	save_file(mun_summary, os.path.join(processed_dir, 'dynamic/vote_count_mun.csv'))
	candidate_summary = pd.concat([reg_summary, prv_summary, mun_summary], axis = 0)
	save_file(candidate_summary, os.path.join(processed_dir, 'dynamic/vote_count.csv'))

	e = time.time()
	print('Total Duration of Preprocessing: ', (e-s)/60,'m and', (e-s)%60,'s')

	local_paths = [os.path.join(processed_dir,'dynamic/vote_type_summary.csv'), os.path.join(processed_dir, 'dynamic/vote_count_reg.csv'), os.path.join(processed_dir, 'dynamic/vote_count_prv.csv'), os.path.join(processed_dir, 'dynamic/vote_count_mun.csv')]
	# local_paths = [os.path.join(processed_dir,'dynamic/vote_type_summary.csv'), os.path.join(processed_dir, 'dynamic/vote_count.csv')]
	return local_paths
	# return os.path.join(processed_dir,'dynamic/vote_type_summary.csv')


if __name__ == '__main__':
	s = time.time()
	unprocessed_dir = os.path.abspath('../../unprocessed')
	processed_dir = os.path.abspath('../../processed')

	codes, candidates, precincts, reg_voters = initialize(unprocessed_dir, processed_dir)
	results = prep_results(unprocessed_dir, precincts)
	# save_file(results, os.path.join(processed_dir, 'dynamic/results_with_turnout.csv'))

	mun_summary = candidate_vote(results, 'PRV_MUN', 'MUNICIPALITY', candidates, codes)
	mun_summary['PRV_NAME'] = mun_summary['PRV_MUN'].apply(lambda x:re.search(r'(.*)-', x).group(1))
	mun_summary['MUN_NAME'] = mun_summary['PRV_MUN'].apply(lambda x:re.search(r'.*-(.*)', x).group(1))
	save_file(mun_summary, os.path.join(processed_dir, 'dynamic/vote_count_mun.csv'))

	# vote_reg = vote_type('REG_NAME', 'REGION', reg_voters, results)
	# vote_prv = vote_type('PRV_NAME', 'PROVINCE', reg_voters, results)
	# vote_mun = vote_type('PRV_MUN', 'MUNICIPALITY', reg_voters, results)
	
	# reg_summary = candidate_vote(results, 'REG_NAME', 'REGION', candidates, codes)
	# prv_summary = candidate_vote(results, 'PRV_NAME', 'PROVINCE', candidates, codes)
	# mun_summary = candidate_vote(results, 'PRV_MUN', 'MUNICIPALITY', candidates, codes)

	# # vote_reg = summarize(results, 'REG_NAME', ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE'])
	# # vote_reg['REGISTERED_VOTERS'] = vote_reg[['REG_NAME']].merge(reg_voters, how = 'left').REGISTERED_VOTERS
	# # vote_reg['TURNOUT'] = (vote_reg['NUMBER_VOTERS']/vote_reg['REGISTERED_VOTERS'])*100
	# # vote_reg = transform(vote_reg, 'REG_NAME', ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE', 'REGISTERED_VOTERS', 'TURNOUT'])
	# # vote_reg['LEVEL'] = 'REGION'

	# # vote_prov = summarize(results, 'PRV_NAME', ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE'])
	# # vote_prov['REGISTERED_VOTERS'] = vote_prov[['PRV_NAME']].merge(reg_voters, how = 'left').REGISTERED_VOTERS
	# # vote_prov['TURNOUT'] = (vote_prov['NUMBER_VOTERS']/vote_prov['REGISTERED_VOTERS'])*100
	# # vote_prov = transform(vote_prov, 'PRV_NAME', ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE', 'REGISTERED_VOTERS', 'TURNOUT'])
	# # vote_prov['LEVEL'] = 'PROVINCE'

	# # vote_mun = summarize(results, 'PRV_MUN', ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE'])
	# # vote_mun['REGISTERED_VOTERS'] = vote_mun[['PRV_MUN']].merge(reg_voters, how = 'left').REGISTERED_VOTERS
	# # vote_mun['TURNOUT'] = (vote_mun['NUMBER_VOTERS']/vote_mun['REGISTERED_VOTERS'])*100
	# # vote_mun['PRV_NAME'] = vote_mun['PRV_MUN'].apply(lambda x:re.search(r'(.*)-', x).group(1))
	# # vote_mun['MUN_NAME'] = vote_mun['PRV_MUN'].apply(lambda x:re.search(r'.*-(.*)', x).group(1))
	# # vote_mun = transform(vote_mun, ['PRV_NAME', 'MUN_NAME'], ['NUMBER_VOTERS', 'UNDERVOTE', 'OVERVOTE', 'REGISTERED_VOTERS', 'TURNOUT'])
	# # vote_mun['LEVEL'] = 'MUNICIPALITY'

	# vote_summary = pd.concat([vote_reg,vote_prv,vote_mun], axis = 0)
	# save_file(vote_summary, os.path.join(processed_dir,'dynamic/vote_type_summary.csv'))

	# # mun_summary = summarize_candidate(results, 'PRV_MUN', candidates.CANDIDATE_NAME.unique())
	# # mun_summary = transform(mun_summary, id_vars = ['PRV_MUN'], value_vars = candidates.CANDIDATE_NAME.unique(), outfile = os.path.join(processed_dir, 'dynamic/candidate_mun.csv'))
	# # mun_summary = add_info(mun_summary, 'MUNICIPALITY', candidates, codes)

	# # prv_summary = summarize_candidate(results, 'PRV_NAME', candidates.CANDIDATE_NAME.unique())
	# # prv_summary = transform(prv_summary, id_vars = ['PRV_NAME'], value_vars = candidates.CANDIDATE_NAME.unique(), outfile = os.path.join(processed_dir, 'dynamic/candidate_prv.csv'))
	# # prv_summary = add_info(prv_summary, 'PROVINCE', candidates, codes)

	# # reg_summary = summarize_candidate(results, 'REG_NAME', candidates.CANDIDATE_NAME.unique())
	# # reg_summary = transform(reg_summary, id_vars = ['REG_NAME'], value_vars = candidates.CANDIDATE_NAME.unique(), outfile = os.path.join(processed_dir, 'dynamic/candidate_reg.csv'))
	# # reg_summary = add_info(reg_summary, 'PROVINCE', candidates, codes)

	# candidate_summary = pd.concat([reg_summary, prv_summary, mun_summary], axis = 0)
	# save_file(candidate_summary, os.path.join(processed_dir, 'dynamic/vote_count.csv'))

	# e = time.time()
	# print('Total Duration of Preprocessing: ', (e-s)/60,'m and', (e-s)%60,'s')
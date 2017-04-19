#!/usr/bin/env python

import glob
import os
import sys, pdb
import pandas
from sqlalchemy import create_engine

engine = create_engine('postgresql://localhost:5432/atlas')

countries = [
	'Argentina',
	'Haiti',
	'United_States',
	'Brazil',
	'Colombia',
	'Mexico',
	'Dominican_Republic',
	'Nicaragua',
	'Ecuador',
	'Panama',
	'El_Salvador',
	'Puerto_Rico',
	'France',
	'Guatemala',
	'USVI'
]

engine.execute('CREATE SCHEMA IF NOT EXISTS zika;')

engine.execute('DROP TABLE IF EXISTS zika.places;')
engine.execute('DROP TABLE IF EXISTS zika.guide;')
engine.execute('DROP TABLE IF EXISTS zika.zika;')

engine.execute("""
	CREATE TABLE zika.places (
		location text, 
		location_type text, 
		country text, 
		state_province text, 
		district_county_municipality text, 
		city text, 
		alt_name1 text, 
		alt_name2 text
	);
""")

engine.execute("""
	CREATE TABLE zika.guide (
		data_field_code text, 
		data_field text, 
		full_description_en text, 
		full_description_original text, 
		report_name text, 
		unit text, 
		time_period_type text
	);
""")

engine.execute("""
	CREATE TABLE zika.zika (
		report_date DATE, 
		location text, 
		location_type text, 
		data_field text, 
		data_field_code text, 
		time_period text, 
		time_period_type text, 
		value INTEGER, 
		unit text
	);
""")

def convert(row):
	for i in range(row.shape[0]):
		if type(row[i]) is str:
			print('%s -> %s' % (row[i], row[i].decode('iso-8859-1').encode('utf-8')))
			row[i] = row[i].decode('latin1').encode('utf-8')

def dump(table, file, encoding='utf-8'):
	try:
		df = pandas.read_csv(file, encoding=encoding)
		df.to_sql(table, engine, schema='zika', index=False, if_exists='append')
	except Exception as e:
		if encoding == 'utf-8':
			dump(table, file, encoding='latin1')
		elif encoding == 'latin1':
			dump(table, file, encoding='iso-8859-1')
		else:
			pdb.set_trace()

for country in countries:
	print(country)
	os.chdir(country)

	dump('guide', glob.glob('*Guide.csv')[0])
	dump('places', glob.glob('*Places.csv')[0])
	
	for f in glob.glob('**/data/*.csv'):
		dump('zika', f)

	os.chdir('../')














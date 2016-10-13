#!/usr/bin/env python

import pandas
import glob
import pdb
import subprocess
import os

guide = pandas.read_csv('../CO_Data_Guide.csv')
stream = open('script.txt', 'w')
files = glob.glob('data/*.csv')
data = pandas.concat(map(pandas.read_csv, files))

data['country'] = data['location'].apply(lambda x: x.split('-')[0])
data['department'] = data['location'].apply(lambda x: x.split('-')[1])
data['municipality'] = data['location'].apply(lambda x: x.split('-')[2])

data.to_csv('all.csv', index=False)

# Zika Data
tableName = 'temp'
createTableCmd = subprocess.check_output(['csvsql', 'all.csv', '--table', tableName])
stream.write('DROP TABLE IF EXISTS %s;\n' % tableName)
stream.write(createTableCmd)
stream.write('COPY %s FROM \'%s\' CSV HEADER;\n' % (tableName, os.getcwd() + '/all.csv'))



# Data Guide
tableName = 'zika_guide'
createTableCmd = subprocess.check_output(['csvsql', '../CO_Data_Guide.csv', '--table', tableName])
stream.write('DROP TABLE IF EXISTS %s;\n' % tableName)
stream.write(createTableCmd)
stream.write('COPY %s FROM \'%s\' CSV HEADER;\n' % (tableName, os.getcwd() + '/../CO_Data_Guide.csv'))


tableName = 'zika'
createTableCmd = subprocess.check_output(['csvsql', 'all.csv', '--table', tableName])
stream.write('DROP TABLE IF EXISTS %s;\n' % tableName)
stream.write("""CREATE TABLE %s (
        report_date DATE NOT NULL,
        country VARCHAR(8) NOT NULL,
        department VARCHAR(15),
        municipality VARCHAR(35)
        );""" % tableName)
stream.write('INSERT INTO zika (SELECT report_date, country, department, municipality FROM temp WHERE data_field_code=\'CO0001\');\n')

for i in range(guide.shape[0]):
	field = guide['data_field'][i]
	stream.write('ALTER TABLE zika ADD COLUMN %s int;\n' % field)
	stream.write('UPDATE zika SET %s=temp.value FROM temp WHERE zika.report_date=temp.report_date AND zika.municipality=temp.municipality;\n' % (field))



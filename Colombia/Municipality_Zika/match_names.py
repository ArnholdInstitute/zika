#!/usr/bin/env python


import json
import pandas
import pdb
from fuzzywuzzy import fuzz
import numpy
import subprocess
import pandas
import os

# Get names from GeoJSON file
geom = json.load(open('./mpio.json'))
geomNames = map(lambda x: x['properties']['NOMBRE_MPI'], geom['features'])
geomNames = set(geomNames)
del(geom)


# Get names from 
csv = pandas.read_csv('../CO_Places.csv')
csv['district_county_municipality'] = csv['district_county_municipality'].apply(lambda x: x.upper() if type(x) is str else x)
csv = csv['district_county_municipality']
csv = csv[~pandas.isnull(csv)]
csvNames = set(csv)

aNotB = list(csvNames.difference(geomNames))
bNotA = list(geomNames.difference(csvNames))

def computeSimilarities():
	mat = numpy.zeros((len(aNotB), len(bNotA)))

	for i in range(len(aNotB)):
		print('Row %d' % i )
		for j in range(len(bNotA)):
			try:
				mat[i, j] = fuzz.partial_ratio(aNotB[i], bNotA[j])
			except Exception as e:
				pass

	numpy.savetxt('matches.csv', mat, delimiter=',')

#computeSimilarities()

mat = numpy.genfromtxt('matches.csv', delimiter=',')
best = mat.max(axis=0)
bestIndex = mat.argmax(axis=0)

stream = open('matches.txt', 'w')
stream.write('geom,csv\n')

for i in range(mat.shape[1]):
	stream.write('%s,%s\n' % (bNotA[i].encode('utf-8'), aNotB[bestIndex[i]]))

stream.flush()

matches = pandas.read_csv('./matches.txt')

scriptStream = open('script.txt', 'w')
tableName = 'names'
scriptStream.write('DROP TABLE IF EXISTS %s;\n' % tableName)
scriptStream.write(subprocess.check_output(['csvsql', os.getcwd() + '/matches.txt', '--tables', tableName]))
scriptStream.write('COPY %s (%s) FROM stdin WITH NULL AS \'nan\';\n' % (tableName, ','.join(matches.columns)))
matches.apply(lambda row: scriptStream.write('\t'.join(map(str, row)) + '\n' ), axis=1)
scriptStream.write('\\.\n')

scriptStream.write('UPDATE columbian_municipalities as c SET municipality=names.csv FROM names WHERE names.geom=c.municipality;\n')

scriptStream.write('DROP TABLE names;\n')



























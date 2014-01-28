import csv
import os
import re
from collections import Counter, OrderedDict

# Vervang door path van te analyzeren run-output
result_path = 'results/runFailedOp10'
ratings_path =  'data/movieData250.txt'
results = open(os.path.join(os.getcwd(), result_path), 'r')
ratings = open(os.path.join(os.getcwd(), ratings_path), 'r')

# fill dict with box office and iMDB rating
movie_metadata_dict = {}
for movie in csv.reader(ratings, delimiter='\t'):
	movie_metadata_dict[movie[0]] = tuple((movie[1], movie[2]))

movie_dict = {}
for movie in csv.reader(results, delimiter='\t'):
	sentiment_dict = {}
	common = Counter(re.findall('-?\d', movie[1])).most_common()
	common = map(lambda (k,v): (int(k),v), common)
  	# overschrijf frequenties
	for k,v in common:
	    sentiment_dict.setdefault(k, []).append(v)
	
	# zet niet voorkomende sentiment categorien naar frequentie 0
	for k in [-2,0,1,2,3,4]:
		if k not in sentiment_dict:
			sentiment_dict.setdefault(k, []).append(0)

  	movie_dict[movie[0]] = OrderedDict(sentiment_dict)

# write result to file
output_path = result_path +'_result'
result_tsv = open(os.path.join(os.getcwd(), output_path), 'w')
# write header row
result_tsv.write('movie_name, #0, #1, #2, #3, #4, #timeout, rating, revenue\n')
# write data rows
for k0, v0 in movie_dict.items():
	result_tsv.write(k0)
	for k1, v1 in v0.items():
		result_tsv.write((', {}').format(v1[0]))
	metaDataTuple = movie_metadata_dict[k0]
	for i in metaDataTuple:
		result_tsv.write((', {}').format(i))
	result_tsv.write('\n')
result_tsv.close()

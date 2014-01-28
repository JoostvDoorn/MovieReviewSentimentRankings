import csv
import os
import re
from collections import Counter, OrderedDict

# Vervang door path van te analyzeren run-output
path = 'results/runFailedOp10'
tsv = open(os.path.join(os.getcwd(), path), 'r')
movie_dict = {}
for movie in csv.reader(tsv, delimiter='\t'):
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
print(movie_dict)

# write result to file
output_path = path+'_result'
result_tsv = open(os.path.join(os.getcwd(), output_path), 'w')
# write header row
result_tsv.write('movie_name, 0, 1, 2, 3, 4, -2\n')
# write data rows
for k0, v0 in movie_dict.items():
	result_tsv.write(k0)
	for k1, v1 in v0.items():
		result_tsv.write((', {}').format(v1[0]))
	result_tsv.write('\n')
result_tsv.close()

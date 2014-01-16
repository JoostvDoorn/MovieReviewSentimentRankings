# MovieReviewSentimentRankings

This collection of Pig scripts can be used to analyse the sentiment on a set of IMDb movies on the Common Crawl web crawl.
The results of the movie sentiment analysis on the web is gathered with the aim to calculate the agreement of movie sentiment on the web with movie sales performance and IMDb rating.
A subset of over 500 000 movies of IMDb is used as input (but note that the current version only considers the top 250).
See report/paper.tex for a more elaborate introduction.

## Getting started with the Pig files
The following Pig scripts are included in this project:

### Cluster_generateGroupedMovies.pig
A Pig script designed to run on the SARA cluster that loads the movie set from the SARA HDFS and performs filtering operations. This filtering includes deletion of movie titles exactly match English words (to prevent noise data that match the movie title in a non-movie related context) and movie titles shorter than 5 characters (as very short character sequences tend to occur very often on the web, often in a non-movie related context). This script stores a DataBag containing all filtered and grouped movies on the SARA HDFS. 

### Cluster_generateGroupedSentimentsByMovie.pig
A Pig script designed to run on the SARA cluster that loads a DataBag with filtered and grouped movies and creates sentiment lists grouped by movie. Run Cluster_generateGroupedMovies.pig first to create the needed DataBag on the SARA HDFS. 

### countSentencesPerMovie.pig
A Pig script designed to run locally to generate sentence counts for each movie. Used during development of the movie list filter, to investigate any additional filters needed.

### generateGroupedSentimentsByMovie.pig
Similar to Cluster_generateGroupedSentimentsByMovie.pig, but it loads the data locally and runs locally. Can be (and is) used for debugging and testing purposes.

### movieTitleExtraction.pig
Old Pig file that tries to create Movie-Sentence pairs and convert them to a Sentiment list per Movie. This Pig file is currently broken and should not be used, generate generateGroupedSentimentsByMovie.pig instead.

### movieTitleExtractionWithAllMoviesCount.pig
Old Pig file that tries to create Movie-Sentence pairs and convert them to a Sentiment list per Movie. This Pig file is currently broken and should not be used, generate generateGroupedSentimentsByMovie.pig instead.

# MovieReviewSentimentRankings

This collection of Pig scripts can be used to analyse the sentiment (using [stanford core nlp](nlp.stanford.edu/software/corenlp.shtml)) on a set of [IMDb](http://www.imdb.com) movies on the [Common Crawl web crawl](http://commoncrawl.org/).
The results of the movie sentiment analysis on the web is gathered with the aim to calculate the agreement of movie sentiment on the web with movie sales performance and IMDb rating.
A subset of over 500 000 movies of IMDb is used as input (but note that the current version only considers the top 250).
See report/paper.tex for a more elaborate introduction.

## Setup
To get started you should have an environment ready which includes pig and hadoop. We used the [Norvig Award VM image](http://norvigaward.github.io/). Before building first run ```./coreNLP.sh``` to download the stanford core nlp library. Now you can build the project (we used Eclipse).

## Pig files
The following Pig scripts are included in this project:

### Cluster scripts
These pig scripts were designed to run on the SARA cluster and contain paths specific to the configuration of that cluster.

#### Cluster_generateGroupedMovies.pig
Loads the movie set from the SARA HDFS and performs filtering operations. This filtering includes deletion of movie titles exactly match English words (to prevent noise data that match the movie title in a non-movie related context) and movie titles shorter than 5 characters (as very short character sequences tend to occur very often on the web, often in a non-movie related context). This script stores a DataBag containing all filtered and grouped movies on the SARA HDFS. 

#### Cluster_generateGroupedSentimentsByMovie.pig
Loads a DataBag with filtered and grouped movies and creates sentiment lists grouped by movie. Run Cluster_generateGroupedMovies.pig first to create the needed DataBag on the SARA HDFS. Since this pig scripts generates multiple map reduce jobs we split them into three seperate pig files.

#### Cluster_generateGroupedSentimentsByMovieFromSentences.pig
Loads tuples of sentences and movies generated in Cluster_generateMovieSentences.pig and performs a sentiment analysis on the sentences using the stanford core nlp library.

#### Cluster_generateMovieSentences.pig
Generates all sentences which contain movie names. Generates null values if the UDF times out.

#### Cluster_removeNullValues.pig
Removes null values from the sentences set.

### Local files
These files were created to test/debug the functionality locally. Paths in these pig scripts match the format in the VM provided by the Norvig Award (the 2012 award).

#### countSentencesPerMovie.pig
Generates sentence counts for each movie. Used during development of the movie list filter, to investigate any additional filters needed.

#### generateGroupedMovies.pig
Similar to Cluster_generateGroupedMovies.pig.

#### generateGroupedSentimentsByMovie.pig
Similar to Cluster_generateGroupedSentimentsByMovie.pig.

#### generateGroupedSentimentsByMovieFromSentences.pig
Similar to Cluster_generateGroupedSentimentsByMovieFromSentences.pig.

#### generateMovieSentences.pig
Similar to Cluster_generateMovieSentences.pig.

#### movieTitleExtraction.pig (deprecated)
Old Pig file that tries to create Movie-Sentence pairs and convert them to a Sentiment list per Movie. This Pig file is currently broken and should not be used, generate generateGroupedSentimentsByMovie.pig instead.

#### movieTitleExtractionWithAllMoviesCount.pig (deprecated)
Old Pig file that tries to create Movie-Sentence pairs and convert them to a Sentiment list per Movie. This Pig file is currently broken and should not be used, generate generateGroupedSentimentsByMovie.pig instead.

## User Defined Functions
Listed Pig Scripts make use of the following Java-based UDF's:

### DocumentFilter.java
A FilterFunc UDF that tries to filter out documents that are not English. It basically checks if the document contains any of a set of common English words.

### IsNotWord.java
A FilterFunc UDF that receives a movie title (as String) and a DataBag of English words. Returns True in movie title string is longer than five characters and does not match an English word, returns false otherwise.

### MoviesInDocument.java
Determines if a movie is contained in the document.

### ToSentenceMoviePairs.java
An EvalFunc UDF that receives a web document (as String) and a DataBag of movie titles. Returns a DataBag of all Tuples of sentences and movie titles in the document where the sentence contains the movie title in the tuple and the movie title is an element of the input movies DataBag.

### ToSentiment.java
A FilterFunc UDF that receives a movie and a sentence. Returns a Tuple of movie and sentiment by applying the StanfordCoreNLP sentiment analysis functionality to the given sentence.

### IsMovieDocument.java (deprecated)
A FilterFunc UDF that receives a web document (as String) and a DataBag of movie titles. Returns True in case the document content contains (case insensitive) one of the movie titles, returns false otherwise. 

### ToMovieSentencePairs.java (deprecated)
An EvalFunc UDF that receives a web document (as String) and a DataBag of movie titles. Returns a DataBag of all Tuples of movie title and sentences in the document where the sentence contains the movie title in the tuple and the movie title is an element of the input movies DataBag.
**Deprecated: replaced by ToSentenceMoviePairs for more favorable distribution over mappers**

# About
This collection of pig scripts and user defined functions is created for the Managing Big Data course at the [University of Twente](http://www.utwente.nl/). We used the [SURFsara hadoop cluster](https://www.surfsara.nl/nl/systems/hadoop) to run our MapReduce jobs in a timely manner. Included is a short paper documenting our method and results.

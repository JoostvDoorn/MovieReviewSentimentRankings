register ../commoncrawl-examples/lib/*.jar; 

register ../commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1-HM.jar;
register ../dist/lib/movierankings-1.jar
register ../lib/piggybank.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1-models.jar;
register ../lib/stanford-corenlp-full-2014-01-04/ejml-0.23.jar;
register ../lib/stanford-corenlp-full-2014-01-04/joda-time.jar;
register ../lib/stanford-corenlp-full-2014-01-04/jollyday.jar;
register ../lib/stanford-corenlp-full-2014-01-04/xom.jar;


DEFINE IsMovieDocument com.moviereviewsentimentrankings.IsMovieDocument;
DEFINE ToMovieSentencePairs com.moviereviewsentimentrankings.ToMovieSentencePairs;
DEFINE ToSentiment com.moviereviewsentimentrankings.ToSentiment;
DEFINE MoviesInDocument com.moviereviewsentimentrankings.MoviesInDocument;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
pages = LOAD '/home/participant/data/textData-*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies = LOAD '../data/movieTitlesTop10.txt' USING PigStorage('\t') as (movie:chararray);

movies_grp = GROUP movies ALL;

movie_pages = FILTER pages BY IsMovieDocument(content, movies_grp.movies);

movie_sentences = FOREACH movie_pages GENERATE flatten(ToMovieSentencePairs(content, movies_grp.movies)) as (movie:chararray, content:chararray);

movie_sentiment = FOREACH movie_sentences GENERATE flatten(ToSentiment(movie, content)) as (movie:chararray, sentiment:int);

movie_sentiment_grp_tups = GROUP movie_sentiment BY movie;

movie_sentiment_grp = FOREACH movie_sentiment_grp_tups GENERATE group, movie_sentiment.sentiment;
dump movie_sentiment_grp;
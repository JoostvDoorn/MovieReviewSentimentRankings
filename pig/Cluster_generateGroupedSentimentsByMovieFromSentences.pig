SET job.name 'Movie Sentiment Extraction';
-- should result in +- 8 mappers. do not set this value much lower, SurfSARA won't like that.
SET pig.maxCombinedSplitSize 9000000;

SET pig.noSplitCombination true
SET mapred.min.split.size 9000000;
SET mapred.max.split.size 9000000;

register ../dist/lib/movierankings-1.jar
register ../lib/piggybank.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1-models.jar;
register ../lib/stanford-corenlp-full-2014-01-04/ejml-0.23.jar;
register ../lib/stanford-corenlp-full-2014-01-04/joda-time.jar;
register ../lib/stanford-corenlp-full-2014-01-04/jollyday.jar;
register ../lib/stanford-corenlp-full-2014-01-04/xom.jar;


DEFINE ToSentiment com.moviereviewsentimentrankings.ToSentiment;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

-- LOAD sentences, movies
movie_sentences = LOAD '/user/utmbd01/results/movie_sentences_filtered/part-*' as (content:chararray, movie:chararray);

-- Calculate sentiment for each movie-sentence pair
movie_sentiment = FOREACH movie_sentences GENERATE flatten(ToSentiment(movie, content)) as (movie:chararray, sentiment:int);

-- GROUP movie-sentiment pairs by movie
movie_sentiment_grp_tups = GROUP movie_sentiment BY movie;

-- Reformat and store movie-sentiment pairs
movie_sentiment_grp = FOREACH movie_sentiment_grp_tups GENERATE group, movie_sentiment.sentiment;
store movie_sentiment_grp INTO 'results/from_sentiments_with_DocumentFilter_all';

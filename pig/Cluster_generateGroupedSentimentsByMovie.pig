set job.name 'Movie Sentiment Extraction'

register ../dist/lib/movierankings-1.jar
register ../lib/piggybank.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1-models.jar;
register ../lib/stanford-corenlp-full-2014-01-04/ejml-0.23.jar;
register ../lib/stanford-corenlp-full-2014-01-04/joda-time.jar;
register ../lib/stanford-corenlp-full-2014-01-04/jollyday.jar;
register ../lib/stanford-corenlp-full-2014-01-04/xom.jar;

DEFINE IsNotWord com.moviereviewsentimentrankings.IsNotWord;
DEFINE IsMovieDocument com.moviereviewsentimentrankings.IsMovieDocument;
DEFINE ToSentenceMoviePairs com.moviereviewsentimentrankings.ToSentenceMoviePairs;
DEFINE ToSentiment com.moviereviewsentimentrankings.ToSentiment;
DEFINE MoviesInDocument com.moviereviewsentimentrankings.MoviesInDocument;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

-- LOAD pages and movies
pages = LOAD '/data/public/common-crawl/parse-output/segment/1346876860648/textData-0002*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies_fltr_grp = LOAD '/user/utmbd01/data/movie_fltr_grp_250/part-*' as (group: chararray,movies_fltr: {(movie: chararray)});

-- FILTER pages containing movie
movie_pages = FILTER pages BY IsMovieDocument(content, movies_fltr_grp.movies_fltr);

-- SPLIT pages containing movie in sentences and create movie-sentence pairs
movie_sentences = FOREACH movie_pages GENERATE flatten(ToSentenceMoviePairs(content, movies_fltr_grp.movies_fltr)) as (content:chararray, movie:chararray);

-- Calculate sentiment for each movie-sentence pair
movie_sentiment = FOREACH movie_sentences GENERATE flatten(ToSentiment(movie, content)) as (movie:chararray, sentiment:int);

-- GROUP movie-sentiment pairs by movie
movie_sentiment_grp_tups = GROUP movie_sentiment BY movie;

-- Reformat and store movie-sentiment pairs
movie_sentiment_grp = FOREACH movie_sentiment_grp_tups GENERATE group, movie_sentiment.sentiment;
store movie_sentiment_grp INTO 'results/movie_sentiment_grp_250';
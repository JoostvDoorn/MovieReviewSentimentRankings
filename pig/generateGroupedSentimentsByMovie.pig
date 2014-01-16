register ../commoncrawl-examples/lib/*.jar; 
set mapred.task.timeout= 1000;
register ../commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1-HM.jar;
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
DEFINE ToMovieSentencePairs com.moviereviewsentimentrankings.ToMovieSentencePairs;
DEFINE ToSentiment com.moviereviewsentimentrankings.ToSentiment;
DEFINE MoviesInDocument com.moviereviewsentimentrankings.MoviesInDocument;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

-- LOAD pages, movies and words
pages = LOAD '../data/textData-*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies = LOAD '../data/movieTitlesTop250.txt' USING PigStorage('\t') as (movie:chararray);
words = LOAD '../data/brit-a-z.txt' USING PigStorage('\t') as (word:chararray);

-- GROUP movies and words
movies_grp = GROUP movies ALL;
words_grp = GROUP words ALL;

-- FILTER movies with English dictionary and title > 5 characters and GROUP result
movies_fltr = FILTER movies BY IsNotWord(movie, words_grp.words);
movies_fltr_grp = GROUP movies_fltr ALL;
dump movies_fltr_grp ;
-- FILTER pages containing movie
movie_pages = FILTER pages BY IsMovieDocument(content, movies_fltr_grp.movies_fltr);
ILLUSTRATE  movie_pages;
-- SPLIT pages containing movie in sentences and create movie-sentence pairs
movie_sentences = FOREACH movie_pages GENERATE flatten(ToMovieSentencePairs(content, movies_fltr_grp.movies_fltr)) as (movie:chararray, content:chararray);
ILLUSTRATE  movie_sentences;
-- Calculate sentiment for each movie-sentence pair
movie_sentiment = FOREACH movie_sentences GENERATE flatten(ToSentiment(movie, content)) as (movie:chararray, sentiment:int);
ILLUSTRATE  movie_sentiment;
-- GROUP movie-sentiment pairs by movie
movie_sentiment_grp_tups = GROUP movie_sentiment BY movie;
ILLUSTRATE  movie_sentiment_grp_tups;
-- Reformat and print movie-sentiment pairs
--movie_sentiment_grp = FOREACH movie_sentiment_grp_tups GENERATE group, movie_sentiment.sentiment;
dump movie_sentiment_grp;
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

DEFINE IsNotWord com.moviereviewsentimentrankings.IsNotWord;
DEFINE IsMovieDocument com.moviereviewsentimentrankings.IsMovieDocument;
DEFINE ToMovieSentencePairs com.moviereviewsentimentrankings.ToMovieSentencePairs;
DEFINE ToSentiment com.moviereviewsentimentrankings.ToSentiment;
DEFINE MoviesInDocument com.moviereviewsentimentrankings.MoviesInDocument;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

-- LOAD pages, movies and words
pages = LOAD '/home/participant/data/textData-*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies = LOAD '../data/movieTitlesTop100.txt' USING PigStorage('\t') as (movie:chararray);
words = LOAD '../data/brit-a-z.txt' USING PigStorage('\t') as (word:chararray);

-- GROUP movies and words
movies_grp = GROUP movies ALL;
words_grp = GROUP words ALL;

-- FILTER movies with English dictionary and title > 5 characters and GROUP result
movies_fltr = FILTER movies BY IsNotWord(movie, words_grp.words);
movies_fltr_grp = GROUP movies_fltr ALL;

-- FILTER pages containing movie
movie_pages = FILTER pages BY IsMovieDocument(content, movies_fltr_grp.movies_fltr);

-- SPLIT pages containing movie in sentences and create movie-sentence pairs
movie_sentences = FOREACH movie_pages GENERATE flatten(ToMovieSentencePairs(content, movies_fltr_grp.movies_fltr)) as (movie:chararray, sentence:chararray);

-- GROUP movie-sentence pairs by movie
movie_sentences_grp_tups = GROUP movie_sentences BY movie;
movie_sentences_grp_tups_cnt = FOREACH movie_sentences_grp_tups GENERATE $1.movie, COUNT($1.sentence);
dump movie_sentences_grp_tups_cnt;

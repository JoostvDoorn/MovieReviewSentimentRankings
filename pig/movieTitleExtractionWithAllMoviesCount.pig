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


DEFINE MoviesInDocument com.moviereviewsentimentrankings.MoviesInDocument;
DEFINE ToSentences com.moviereviewsentimentrankings.ToSentences;
DEFINE MovieSentences com.moviereviewsentimentrankings.MovieSentences;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
pages = LOAD '/home/participant/data/textData-*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies = LOAD '../data/movieTitlesTop250.txt' USING PigStorage('\t') as (movie:chararray);

movies_grp         = GROUP movies ALL;
movie_pages = FOREACH pages GENERATE FLATTEN(MoviesInDocument(content, movies_grp.movies)) as (content, movie);

movie_pages_grp = GROUP movie_pages BY movie;
movie_page_count = FOREACH movie_pages_grp GENERATE group, COUNT(movie_pages.content);
dump movie_page_count;
total_count = FOREACH movie_page_count GENERATE SUM($1);
dump total_count;

movie_page_sentences = FOREACH movie_pages GENERATE flatten(ToSentences(content)) as (movie:chararray, content:chararray);

movie_sentences = FILTER movie_page_sentences BY IsMovieDocument(content, movies_grp.movies);

movie_pages_sentences_grp = GROUP movie_page_sentences BY movie;
movie_page_sentences_count = FOREACH movie_pages_sentences_grp GENERATE group, COUNT(movie_pages_sentences.content);
dump movie_page_sentences_count;
total_count_sentences = FOREACH movie_page_sentences_count GENERATE SUM($1);
dump total_count_sentences;
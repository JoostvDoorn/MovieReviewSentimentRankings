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
DEFINE ToSentences com.moviereviewsentimentrankings.ToSentences;
DEFINE MovieSentences com.moviereviewsentimentrankings.MovieSentences;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
pages = LOAD '/home/participant/data/textData-*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies = LOAD '../data/movieTitlesTop250.txt' USING PigStorage('\t') as (movie:chararray);

movies_grp         = GROUP movies ALL;
movie_page_sentences = FOREACH pages GENERATE content, 
                        IsMovieDocument(content, movies_grp.movies) AS movie;

-- movie_page_sentences = FOREACH pages GENERATE FLATTEN(MovieSentences(movies, content));

dump movie_page_sentences;
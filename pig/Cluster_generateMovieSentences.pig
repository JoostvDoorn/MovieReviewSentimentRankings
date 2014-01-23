set job.name 'Movie Sentence Extraction'

register ../dist/lib/movierankings-1.jar;
register ../lib/*.jar;
register ../lib/stanford-corenlp-full-2014-01-04/stanford-corenlp-3.3.1.jar;
register ../lib/stanford-corenlp-full-2014-01-04/ejml-0.23.jar;
register ../lib/stanford-corenlp-full-2014-01-04/joda-time.jar;
register ../lib/stanford-corenlp-full-2014-01-04/jollyday.jar;
register ../lib/stanford-corenlp-full-2014-01-04/xom.jar;

DEFINE IsNotWord com.moviereviewsentimentrankings.IsNotWord;
DEFINE IsMovieDocument com.moviereviewsentimentrankings.IsMovieDocument;
DEFINE ToSentenceMoviePairs com.moviereviewsentimentrankings.ToSentenceMoviePairs;
DEFINE MoviesInDocument com.moviereviewsentimentrankings.MoviesInDocument;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

-- LOAD pages, movies and words
pages = LOAD '/data/public/common-crawl/parse-output/segment/1346876860648/textData-00*' USING SequenceFileLoader as (url:chararray, content:chararray);
movies_fltr_grp = LOAD '/user/utmbd01/data/movie_fltr_grp/part-*' as (group: chararray,movies_fltr: {(movie: chararray)});

-- FILTER pages containing movie
movie_pages = FILTER pages BY IsMovieDocument(content, movies_fltr_grp.movies_fltr);

-- SPLIT pages containing movie in sentences and create movie-sentence pairs
movie_sentences = FOREACH movie_pages GENERATE flatten(ToSentenceMoviePairs(content, movies_fltr_grp.movies_fltr)) as (content:chararray, movie:chararray);

-- movie_sentences: {content: chararray,movie: chararray}
store movie_sentences INTO 'results/movie_sentences2';

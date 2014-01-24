register ../dist/lib/movierankings-1.jar;

DEFINE IsNotWord com.moviereviewsentimentrankings.IsNotWord;
DEFINE IsMovieDocument com.moviereviewsentimentrankings.IsMovieDocument;

-- LOAD pages, movies and words
movies = LOAD '../data/movieTitlesTop250.txt' USING PigStorage('\t') as (movie:chararray);

words = LOAD '../data/brit-a-z.txt' USING PigStorage('\t') as (word:chararray);

-- GROUP movies and words
movies_grp = GROUP movies ALL;
words_grp = GROUP words ALL;

-- FILTER movies with English dictionary and title > 5 characters and GROUP result
movies_fltr = FILTER movies BY IsNotWord(movie, words_grp.words);
movies_fltr_grp = GROUP movies_fltr ALL;
store movies_fltr_grp INTO '../data/movie_fltr_grp';

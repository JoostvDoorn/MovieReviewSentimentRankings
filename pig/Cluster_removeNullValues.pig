movie_sentences = LOAD 'results/movie_sentences_run1/part-m-*' as (content:chararray, movie:chararray);
movie_sentences_not_nulls = FILTER movie_sentences BY content is not null AND movie is not null;
-- movie_sentences: {content: chararray,movie: chararray}
store movie_sentences_not_nulls INTO 'results/movie_sentences_filtered';


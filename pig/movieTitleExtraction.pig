register ../commoncrawl-examples/lib/*.jar; 

register ../commoncrawl-examples/dist/lib/commoncrawl-examples-1.0.1-HM.jar;
register ../dist/lib/movierankings-1.jar;
register ../lib/piggybank.jar;
DEFINE findSentences com.moviereviewsentimentrankings.FindSentences;
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();
pages = LOAD '/home/participant/data/textData-*' USING SequenceFileLoader as (url:chararray, contents:chararray);

sentences = foreach pages generate findSentences(url, contents);

dump sentences;

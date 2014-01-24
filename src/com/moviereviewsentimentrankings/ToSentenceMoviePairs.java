package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.moviereviewsentimentrankings.IsMovieDocument.BooleanHolder;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class ToSentenceMoviePairs extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		int timeout = 3000; // in ms
		
		if(input.get(0) == null) {
			throw new IOException("Expected input to be chararray, but  got null");
		}
		if (!(input.get(0) instanceof String)) {
			throw new IOException("Expected input to be chararray, but  got " + (input.get(0) == null ? "" : input.get(0).getClass().getName()));
		}
		final String content 		= (String) input.get(0);
		final DataBag movieList 	= (DataBag) input.get(1);
		
		final TupleFactory mTupFactory = TupleFactory.getInstance();
		final BagFactory mBagFactory = BagFactory.getInstance();
		final DataBag movieSentenceTuples = mBagFactory.newDefaultBag();
		
		Future<?> future = executor.submit(new Runnable() {
			public void run(){

				reporter.progress();
			
				// Document op zinnen splitsen
				DocumentPreprocessor dp = new DocumentPreprocessor(new StringReader(content));
				Iterator<List<HasWord>> it = dp.iterator();
				while (it.hasNext()) {
				   StringBuilder sentenceSb = new StringBuilder();
				   List<HasWord> sentence = it.next();
				   for (HasWord token : sentence) {
				      if(sentenceSb.length()>1) {
				         sentenceSb.append(" ");
				      }
				      sentenceSb.append(token);
				   }
				   String sentenceS = sentenceSb.toString();
				   // Only look at documents with sentences shorter than 300 characters
				   if (sentenceS.length() < 300){
				   // Document checken op filmnaam
						for(Tuple movie: movieList){
							reporter.progress();
							String title = null;
							try {
								title = (String) movie.get(0);
							} catch(ExecException e) {
								
							}
							if(sentenceS != null && title != null && sentenceS.toLowerCase().contains(" "+title.toLowerCase()+" ")) {
								Tuple tuple = mTupFactory.newTuple();
								tuple.append(sentenceS);
								tuple.append(title);
								movieSentenceTuples.add(tuple);
							}
						}
					}
				}
			}
		});
		
		executor.shutdown();
		try {
			future.get(timeout, TimeUnit.MILLISECONDS);
		} catch(InterruptedException e){
		} catch(ExecutionException e){
			future.cancel(true);
		} catch(TimeoutException e){
			future.cancel(true);
		}
		
		
			
		// Return whether doc contains movie title
        return movieSentenceTuples;
		
	}

}

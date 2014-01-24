package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class IsMovieDocument extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		int timeout = 3000; // in ms
		
		final String content = (String) input.get(0);		
		final DataBag movieList = (DataBag) input.get(1);
		final BooleanHolder docContainsMovie = new BooleanHolder();
		docContainsMovie.value = false;
		
		Future<?> future = executor.submit(new Runnable() {
			public void run(){

				reporter.progress();
				// Document checken op filmnaam
				
				for(Tuple movie: movieList){
					String title;
					try {
						title = (String) movie.get(0);
					} catch (ExecException e) {
						continue;
					}
					if(content != null && title != null && content.toLowerCase().contains(" "+title.toLowerCase()+" ")){
						docContainsMovie.value = true;
						break;
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
        return docContainsMovie.value;
	}
	
	class BooleanHolder {
	    protected boolean value;
	}
}

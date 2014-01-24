package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class DocumentFilter extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(1);
		int timeout = 3000; // in ms
		
		final String content = (String) input.get(0);		
		final DataBag movieList = (DataBag) input.get(1);
		final BooleanHolder englishAndMovie = new BooleanHolder();
		englishAndMovie.value = false;
		
		Future<?> future = executor.submit(new Runnable() {
			public void run(){
				
				reporter.progress();
				
				String lcContent = null;
				// Create lowercase content
				if(content != null )
					lcContent = content.toLowerCase();

				if(lcContent != null){
					// Document checken op top 5 english words
					boolean english = false;
					String words[] = new String[] {"the","be","to","of","and"};
					for(String word:words){
						if(lcContent.contains(" "+word+" ")){
							english = true;
							break;
						}
					}
					
					if(english){
					// Document checken op filmnaam
						for(Tuple movie: movieList){
							String title;
							try {
								title = (String) movie.get(0);
							} catch (ExecException e) {
								continue;
							}
							if(content != null && title != null && content.toLowerCase().contains(" "+title.toLowerCase()+" ")){
								englishAndMovie.value = true;
								break;
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
        return englishAndMovie.value;
	}
	
	class BooleanHolder {
	    protected boolean value;
	}
}

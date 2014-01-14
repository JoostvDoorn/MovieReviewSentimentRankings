package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class IsMovieDocument extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
        try {
    		String content = (String) input.get(0);
    		
			DataBag movieList = (DataBag) input.get(1);

			// Document checken op filmnaam
			boolean docContainsMovie = false;
			for(Tuple movie: movieList){
				String title = (String) movie.get(0);
				if(content.toLowerCase().contains(title.toLowerCase())){
					docContainsMovie = true;
					break;
				}
			}
			
			// Return whether doc contains movie title
            return docContainsMovie;
        } catch (ExecException ee) {
            throw new IOException("Caught exception processing input row ", ee);   
        }

	}
}

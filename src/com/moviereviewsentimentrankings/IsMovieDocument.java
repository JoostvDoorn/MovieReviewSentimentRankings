package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class IsMovieDocument extends FilterFunc {

	@Override
	public Boolean exec(Tuple input) throws IOException {
        try {
    		String content = (String) input.get(0);
        	// Movie List
			// TODO: Movie file inlezen/doorpassen
			List<String> movieList = new ArrayList<String>(
				    Arrays.asList("The Godfather", "Toy Story", "The Matrix", "Jurassic Park", "Home Alone", "Star Wars", "The Lord of the Rings"));

			// Document checken op filmnaam
			boolean docContainsMovie = false;
			for(String movie: movieList){
				if(content.toLowerCase().contains(movie.toLowerCase())){
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

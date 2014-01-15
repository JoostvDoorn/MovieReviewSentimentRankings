package com.moviereviewsentimentrankings;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class IsNotWord extends FilterFunc{

	@Override
	public Boolean exec(Tuple input) throws IOException {
		if(input.get(0) == null) {
			throw new IOException("Expected input to be chararray, but  got null");
		}
		if (!(input.get(0) instanceof String)) {
			throw new IOException("Expected input to be chararray, but  got " + (input.get(0) == null ? "" : input.get(0).getClass().getName()));
		}
		String movie 		= (String) input.get(0);
		DataBag wordList 	= (DataBag) input.get(1);
		
		boolean matchesWord = false;
		// Test if movie title > 5 characters
		boolean minLength	= movie.length() > 5;
		
		if(minLength)
			for(Tuple wordTuple: wordList){
				String word = (String) wordTuple.get(0);
				if(movie == null || (word != null && word.toLowerCase().equals(movie.toLowerCase()))){
					matchesWord = true;
					break;
				}			
			}
		
		return minLength && !matchesWord;
	}
}

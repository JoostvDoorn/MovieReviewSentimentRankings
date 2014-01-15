package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class MoviesInDocument extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		try{
			TupleFactory mTupFactory = TupleFactory.getInstance();
			BagFactory mBagFactory = BagFactory.getInstance();
			DataBag movieDocuments = mBagFactory.newDefaultBag();
			
			if(input.get(0) == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(input.get(0) instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (input.get(0) == null ? "" : input.get(0).getClass().getName()));
			}
			if(input.get(1) == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(input.get(1) instanceof DataBag)) {
				throw new IOException("Expected input to be databag, but  got " + (input.get(1) == null ? "" : input.get(1).getClass().getName()));
			}
			String content = (String) input.get(0);
			DataBag movieList = (DataBag) input.get(1);

			for(Tuple movie: movieList){
				String title = (String) movie.get(0);
				if(content.toLowerCase().contains(title.toLowerCase())){
					Tuple tuple = mTupFactory.newTuple();
					tuple.append(title);
					tuple.append(content);
					movieDocuments.add(tuple);
				}
			}

			return movieDocuments;
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}

}

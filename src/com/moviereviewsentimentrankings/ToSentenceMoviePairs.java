package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class ToSentenceMoviePairs extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		try{
			TupleFactory mTupFactory = TupleFactory.getInstance();
			BagFactory mBagFactory = BagFactory.getInstance();
			DataBag movieSentenceTuples = mBagFactory.newDefaultBag();
						
			if(input.get(0) == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(input.get(0) instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (input.get(0) == null ? "" : input.get(0).getClass().getName()));
			}
			String content 		= (String) input.get(0);
			DataBag movieList 	= (DataBag) input.get(1);
			
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
						String title = (String) movie.get(0);
						if(sentenceS != null && title != null && sentenceS.toLowerCase().contains(" "+title.toLowerCase()+" ")) {
							Tuple tuple = mTupFactory.newTuple();
							tuple.append(sentenceS);
							tuple.append(title);
							movieSentenceTuples.add(tuple);
						}
					}
				}
			}
				
			return movieSentenceTuples;
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}

}

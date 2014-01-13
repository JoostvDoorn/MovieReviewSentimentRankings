package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.io.StringReader;
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

public class ToSentences extends EvalFunc<DataBag> {

	@Override
	public DataBag exec(Tuple input) throws IOException {
		try{
			TupleFactory mTupFactory = TupleFactory.getInstance();
			BagFactory mBagFactory = BagFactory.getInstance();
			DataBag sentences = mBagFactory.newDefaultBag();
			
			if(input.get(0) == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(input.get(0) instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (input.get(0) == null ? "" : input.get(0).getClass().getName()));
			}
			String content = (String) input.get(0);
			
			// Document op zinnen splitsen
			DocumentPreprocessor dp = new DocumentPreprocessor(new StringReader(content));
			List<String> sentenceList = new LinkedList<String>();
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
			   sentenceList.add(sentenceSb.toString());
			}
			
			for(String sentence: sentenceList){
				Tuple tuple = mTupFactory.newTuple();
				String movie = Math.random() > 0.5 ? "Toy Story" : "The Matrix";
				tuple.append(movie);
				tuple.append(sentence);
				sentences.add(tuple);
			}
				
			return sentences;
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}

}

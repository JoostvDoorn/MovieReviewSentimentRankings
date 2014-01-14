package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class ToSentiment extends EvalFunc<Tuple> {
	Properties props;
	StanfordCoreNLP pipeline;
	
	public ToSentiment(){
		props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		pipeline = new StanfordCoreNLP(props);
	}
	
	@Override
	public Tuple exec(Tuple input) throws IOException {
		try{
			TupleFactory mTupFactory = TupleFactory.getInstance();
			Tuple tuple = mTupFactory.newTuple();
			
			if(input.get(0) == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(input.get(0) instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (input.get(0) == null ? "" : input.get(0).getClass().getName()));
			}
			if(input.get(1) == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(input.get(1) instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (input.get(1) == null ? "" : input.get(1).getClass().getName()));
			}
			String movie   = (String) input.get(0);
			String content = (String) input.get(1);
			
			// Zin afbreken als > x characters (TODO: tijdelijk x=300, beter over nadenken)
			if (content.length() > 300){
			    tuple.append(movie);
			    // Model to long documents with -1
			    tuple.append(-1);
				return tuple;
			}
			
			// Zin sentiment opvragen
			int mainSentiment = 0;
		    if (content != null && content.length() > 0) {
		    	int longest = 0;
		        Annotation annotation = pipeline.process(content);
		        for (CoreMap cm : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
		        	Tree tree = cm.get(SentimentCoreAnnotations.AnnotatedTree.class);
		            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
		            String partText = content.toString();
		            if (partText.length() > longest) {
		            	mainSentiment = sentiment;
		                longest = partText.length();
		            }
		        }
		    }
		    
		    tuple.append(movie);
		    tuple.append(mainSentiment);
			return tuple;
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
	}
}

package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
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
		ExecutorService executor = Executors.newFixedThreadPool(1);
		int timeout = 3000; // in ms
		final TupleFactory mTupFactory = TupleFactory.getInstance();
		
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
		final String movie   = (String) input.get(0);
		final String content = (String) input.get(1);
		final Tuple tuple = mTupFactory.newTuple();
		
		Future<?> future = executor.submit(new Runnable() {
			public void run(){
				
				// Zin afbreken als > x characters (TODO: tijdelijk x=300, beter over nadenken)
				if (content.length() > 300){
				    tuple.append(movie);
				    // Model to long documents with -1
				    tuple.append(-1);
				}
				
				// Zin sentiment opvragen
				int mainSentiment = 0;
			    if (content != null && content.length() > 0) {
			    	int longest = 0;
			        Annotation annotation = pipeline.process(content);
			        for (CoreMap cm : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
			            reporter.progress();
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
			}
		});
		executor.shutdown();
		try {
			future.get(timeout, TimeUnit.MILLISECONDS);
		} catch(InterruptedException e){
		} catch(ExecutionException e){
			future.cancel(true);
			tuple.append(movie);
			tuple.append(-2);
		} catch(TimeoutException e){
			future.cancel(true);
			tuple.append(movie);
			tuple.append(-2);
		}
		
		return tuple;
	}
}

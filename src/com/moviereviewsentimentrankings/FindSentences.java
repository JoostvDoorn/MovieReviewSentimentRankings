package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


public class FindSentences extends EvalFunc<DataBag> {
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();

	public DataBag exec(Tuple input) throws IOException {
		try {
			DataBag output = mBagFactory.newDefaultBag();
			Object o_url = input.get(0);
			Object o_contents = input.get(1);
			if(o_contents == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(o_contents instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (o_contents == null ? "" : o_contents.getClass().getName()));
			}
			if(o_url == null) {
				throw new IOException("Expected input to be chararray, but  got null");
			}
			if (!(o_url instanceof String)) {
				throw new IOException("Expected input to be chararray, but  got " + (o_url == null ? "" : o_url.getClass().getName()));
			}
			String url = (String) o_url;
			String contents = (String) o_contents;

			Properties props = new Properties();

		    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		    StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		    Annotation annotation = new Annotation(contents);
		    pipeline.annotate(annotation);
		    for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
		    	Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
		    	String sentence_result = sentence.get(SentimentCoreAnnotations.ClassName.class);
		    	output.add(mTupleFactory.newTuple(Arrays.asList(url,sentence,sentence_result)));
		    }
			
			return output;
		} catch (ExecException ee) {
			throw WrappedIOException.wrap("Caught exception processing input row ", ee);
		}
	}
}
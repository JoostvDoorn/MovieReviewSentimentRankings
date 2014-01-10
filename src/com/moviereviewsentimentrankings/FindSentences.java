package com.moviereviewsentimentrankings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;

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
			String[] sentences = contents.split("([.?!][ \n\t])");
			for(String sentence : sentences) {
				output.add(mTupleFactory.newTuple(Arrays.asList(url, sentence)));
			}
			
			return output;
		} catch (ExecException ee) {
			throw WrappedIOException.wrap("Caught exception processing input row ", ee);
		}
	}
}
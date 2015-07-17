package edu.missouri.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreMedianReducer extends
		Reducer<DoubleWritable, IntWritable, Text, Text> {

	@Override
	public void reduce(DoubleWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		if (Double.parseDouble(key.toString()) == -1) {
			context.write(new Text("SIZE"), new Text(key.toString()));
		} else {
			Iterator<IntWritable> it = values.iterator();
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count += 1;
			}

			context.write(new Text("median:"+key + "," + count), new Text(""));
		}
	}
}

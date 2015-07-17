package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreMedianMapper extends
		Mapper<Object, Text, DoubleWritable, IntWritable> {
	int count = 0;

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		count++;
		context.write(new DoubleWritable(Double.parseDouble(value.toString())),
				new IntWritable(1));
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		context.write(new DoubleWritable(-1), new IntWritable(count));
	}
}

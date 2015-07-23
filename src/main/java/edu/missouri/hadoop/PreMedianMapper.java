package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
		context.write(new DoubleWritable(Double.parseDouble(value.toString().split(",")[1])),
				new IntWritable(1));
	}

	// To transfer the total number across different mappers and reducers
	// simply because the key is impossible to be zero
	/*
	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		//context.write(new DoubleWritable(-1), new IntWritable(count));
		context.getConfiguration().setInt("size", count);
	}
	*/
}

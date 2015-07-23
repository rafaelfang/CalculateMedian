package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetMedianMapper extends
		Mapper<Object, Text, DoubleWritable, IntWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
			String str[]=value.toString().split("\\t");
			context.write(new DoubleWritable(Double.parseDouble(str[0])), new IntWritable(Integer.parseInt(str[1])));
	}
}

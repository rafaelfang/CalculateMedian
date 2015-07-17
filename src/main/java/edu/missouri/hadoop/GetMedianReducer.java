package edu.missouri.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class GetMedianReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		
		String tempKey;
		String tempVal;
		
		while (it.hasNext()) {
			String tempStrArr[] = it.next().toString().split(",");
			tempKey=tempStrArr[0];
			tempVal=tempStrArr[1];
			
		}
		
		totalWordCount.set(wordCount);
		context.write(key, totalWordCount);
	}
}

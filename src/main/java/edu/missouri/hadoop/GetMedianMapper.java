package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetMedianMapper extends
		Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		
		String arr[]=value.toString().split(":");
		if(arr[0].equals("median")){
			String temp[]=arr[1].split(",");
			context.write(new Text(temp[0]),
				new Text(temp[1]));
		}else{
			context.write(new Text(arr[0]),
					new Text(arr[1]));
		}
		
	}
}

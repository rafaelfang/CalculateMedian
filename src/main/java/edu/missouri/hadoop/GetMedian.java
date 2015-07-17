package edu.missouri.hadoop;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GetMedian {
	

	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job preJob = Job.getInstance(new Configuration());

		preJob.setOutputKeyClass(DoubleWritable.class);
		preJob.setOutputValueClass(IntWritable.class);

		preJob.setMapperClass(PreMedianMapper.class);
		preJob.setReducerClass(PreMedianReducer.class);

		preJob.setInputFormatClass(TextInputFormat.class);
		preJob.setOutputFormatClass(TextOutputFormat.class);

		preJob.setJarByClass(GetMedian.class);

		FileInputFormat.setInputPaths(preJob, new Path(args[0]));
		Path tempDir = new Path("median-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		FileOutputFormat.setOutputPath(preJob, tempDir);

		// job.submit();
		if (preJob.waitForCompletion(true)) {
			Job getMedian = Job.getInstance(new Configuration());
			getMedian.setNumReduceTasks(1);

			getMedian.setOutputKeyClass(Text.class);
			getMedian.setOutputValueClass(Text.class);

			getMedian.setMapperClass(GetMedianMapper.class);
			getMedian.setReducerClass(GetMedianReducer.class);

			getMedian.setInputFormatClass(TextInputFormat.class);
			getMedian.setOutputFormatClass(TextOutputFormat.class);

			getMedian.setJarByClass(GetMedian.class);
		
			FileInputFormat.setInputPaths(getMedian, tempDir);
			FileOutputFormat.setOutputPath(getMedian, new Path(args[1]));

			System.exit(getMedian.waitForCompletion(true) ? 0 : 1);
		}
	}
}

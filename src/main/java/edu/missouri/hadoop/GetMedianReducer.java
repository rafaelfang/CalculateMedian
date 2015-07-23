package edu.missouri.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GetMedianReducer extends Reducer<DoubleWritable, IntWritable, Text, Text> {

	private int size = 0;
	private HashMap<Double, Integer> hm = new HashMap<Double, Integer>();

	@Override
	public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int total = 0;
		Iterator<IntWritable> it = values.iterator();

		while (it.hasNext()) {
			int temp = it.next().get();
			size += temp;
			total += temp;
		}
		hm.put(key.get(), total);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		Iterator it = hm.entrySet().iterator();
		int total = 0;
		double for_two = 0;
		boolean isOne = true;

		while (it.hasNext()) {
			Map.Entry<Double, Integer> pair = (Map.Entry)it.next();
			total += pair.getValue();
			if (total > size / 2) {
				context.write(new Text(pair.getKey().toString()), new Text("median"));
				break;
			} else if (total == size / 2) {
				if (size % 2 == 0) {
					context.write(new Text(pair.getKey().toString()), new Text("median"));
					break;
				}
				else {
					for_two += pair.getKey();
					if (isOne)
						continue;
					else {
						context.write(new Text(Double.toString(for_two / 2)), new Text("median"));
						break;
					}
				}
			}
		}
		context.write(new Text("The size is: "), new Text(Integer.toString(size)));
	}
}

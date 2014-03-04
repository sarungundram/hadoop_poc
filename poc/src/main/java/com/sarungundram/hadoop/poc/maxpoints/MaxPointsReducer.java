package com.sarungundram.hadoop.poc.maxpoints;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxPointsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private int maxPoints = Integer.MIN_VALUE;
	private String team = null;

	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int total = 0;
		for (IntWritable value : values) {
			++total;
		}
		System.out.println(key + "\t" + total);
		if (total > maxPoints) {
			team = key.toString().split("\\s+")[1];
			maxPoints = total;
		}
		context.write(key, new IntWritable(total));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (team != null)
			context.write(new Text(team), new IntWritable(maxPoints));
		super.cleanup(context);
	}

}

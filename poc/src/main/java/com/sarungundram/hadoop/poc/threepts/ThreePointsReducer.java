package com.sarungundram.hadoop.poc.threepts;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThreePointsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private int max = Integer.MIN_VALUE;
	private String player = null;

	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int total3pts = 0;
		for (IntWritable value : values) {
			++total3pts;
		}
		if (total3pts > max) {
			player = key.toString();
			max = total3pts;
		}
		if (total3pts > 0)
		context.write(key, new IntWritable(total3pts));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (player != null)
			context.write(new Text(player), new IntWritable(max));
		super.cleanup(context);
	}

}

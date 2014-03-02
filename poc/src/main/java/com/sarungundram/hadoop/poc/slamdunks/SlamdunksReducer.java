package com.sarungundram.hadoop.poc.slamdunks;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SlamdunksReducer extends Reducer<NullWritable, Text, Text, Text> {

	protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		int total = 0, made = 0;
		for (Text value : values) {
			++total;
			if ("made".equals(value.toString())) {
				++made;
			}
		}
		context.write(new Text(context.getConfiguration().get("player.name")),
				new Text(String.format("%d out of %d", made, total)));
	}

}

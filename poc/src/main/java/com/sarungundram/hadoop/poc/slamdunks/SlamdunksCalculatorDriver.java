package com.sarungundram.hadoop.poc.slamdunks;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sarungundram.hadoop.poc.PlayFileInputFormat;

public class SlamdunksCalculatorDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Slam Dunks");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(PlayFileInputFormat.class);
		job.getConfiguration().set("player.name", args[0]);
		Path inputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path(args[2]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(SlamdunksMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(SlamdunksReducer.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SlamdunksCalculatorDriver(), args);
		System.exit(exitCode);
	}
}

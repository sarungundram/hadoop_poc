package com.sarungundram.hadoop.poc.homegames;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sarungundram.hadoop.poc.PlayFileInputFormat;
import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class MostGameWinsCalculatorDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Most Home Game Wins");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(PlayFileInputFormat.class);
		Path inputPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inputPath);
		
		Path outputPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(MostHomeGameWinsMapper.class);
		job.setMapOutputKeyClass(PlayKey.class);
		job.setMapOutputValueClass(Play.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(MostHomeGameWinsReducer.class);
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MostGameWinsCalculatorDriver(), args);
		System.exit(exitCode);
	}
}

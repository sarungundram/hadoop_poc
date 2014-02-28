package com.sarungundram.hadoop.poc.freethrows;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sarungundram.hadoop.poc.PlayFileInputFormat;

public class LBJFreeThrowPctCalculatorDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "LBJ Free Throw %");
		job.setJarByClass(getClass());
		
		job.setInputFormatClass(PlayFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(LBJFreeThrowPctMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new LBJFreeThrowPctCalculatorDriver(), args);
		System.exit(exitCode);
	}
}

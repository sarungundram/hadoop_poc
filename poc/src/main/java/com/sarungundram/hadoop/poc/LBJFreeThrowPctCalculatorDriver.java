package com.sarungundram.hadoop.poc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class LBJFreeThrowPctCalculatorDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "LBJ Free Throw %");

		return 0;
	}
}

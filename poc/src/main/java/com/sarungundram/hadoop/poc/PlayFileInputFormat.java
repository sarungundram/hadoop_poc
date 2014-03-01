package com.sarungundram.hadoop.poc;

import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class PlayFileInputFormat extends CombineFileInputFormat<PlayKey, Play> {

	@Override
	public RecordReader<PlayKey, Play> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<PlayKey, Play>((CombineFileSplit) split, context, PlayFileRecordReader.class);
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	public static void main(String[] args) {
		Matcher matcher = PlayFileRecordReader.fileNamePattern.matcher("20081129.CLEMIL.csv");
		if (matcher.matches()) {
			System.out.println(matcher.group(1) + "\t" + matcher.group(2) + "\t" + matcher.group(3));
		}
	}

}

package com.sarungundram.hadoop.poc.homegames;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class MostHomeGameWinsMapper extends Mapper<PlayKey, Play, PlayKey, Play> {

	@Override
	protected void map(PlayKey key, Play value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}

package com.sarungundram.hadoop.poc.fouls;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class FoulsMapper extends Mapper<PlayKey, Play, Text, IntWritable> {

	@Override
	protected void map(PlayKey key, Play value, Context context) throws IOException, InterruptedException {
		Text player = value.getPlayer();
		Text etype = value.getEtype();
		if (etype != null && "foul".equals(etype.toString())) {
			context.write(new Text(player.toString() + "(" + value.getTeamValue() + ")"), new IntWritable(1));
		}
	}
}

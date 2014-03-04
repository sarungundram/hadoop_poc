package com.sarungundram.hadoop.poc.maxpoints;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class MaxPointsMapper extends Mapper<PlayKey, Play, Text, IntWritable> {

	@Override
	protected void map(PlayKey key, Play value, Context context) throws IOException, InterruptedException {
		IntWritable points = value.getPoints();
		Text gameDate = key.getDate();
		
		Text outputKey = new Text(gameDate.toString() + " " + value.getTeamValue());
		if ((points != null && points.get() != 0) && (value.getPeriod().get() == 1 || value.getPeriod().get() == 2)) {
			context.write(outputKey, points);
		}else if ("free throw".equals(value.getEtypeValue()) && "made".equals(value.getResultValue())) {
			context.write(outputKey, new IntWritable(1));
		}
	}
}

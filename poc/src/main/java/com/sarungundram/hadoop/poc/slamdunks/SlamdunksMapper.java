package com.sarungundram.hadoop.poc.slamdunks;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class SlamdunksMapper extends Mapper<PlayKey, Play, NullWritable, Text> {

	@Override
	protected void map(PlayKey key, Play value, Context context) throws IOException, InterruptedException {
		String inputPlayer = context.getConfiguration().get("player.name");
		if ("slam dunk".equals(value.getTypeValue())
				&& value.getPlayerValue().equalsIgnoreCase(inputPlayer)) {
			System.out.println(key.getHomeTeam() + " vs " + key.getAwayTeam() + " - " + value.getResultValue());
			context.write(NullWritable.get(), value.getResult());
		}
	}
}

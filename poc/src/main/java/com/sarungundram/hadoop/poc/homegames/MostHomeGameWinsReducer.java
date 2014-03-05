package com.sarungundram.hadoop.poc.homegames;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class MostHomeGameWinsReducer extends Reducer<PlayKey, Play, Text, IntWritable> {
	Map<String, Integer> teamWinningsMap = new HashMap<String, Integer>();

	protected void reduce(PlayKey key, Iterable<Play> values, Context context) throws IOException,
			InterruptedException {
		int homeTeamTotalPoints = 0, awayTeamTotalPoints = 0;
		for (Play value : values) {
			int pointsMade = 0;
				IntWritable points = value.getPoints();
				if ((points != null && points.get() != 0)) {
					pointsMade = points.get();
				} else if ("free throw".equals(value.getEtypeValue()) && "made".equals(value.getResultValue())) {
					pointsMade = 1;
				}
			if (key.getHomeTeam().toString().equals(value.getTeamValue())) {
				homeTeamTotalPoints += pointsMade;
			}else {
				awayTeamTotalPoints += pointsMade;
			}
		}
		String winningTeam = null;
		if (homeTeamTotalPoints > awayTeamTotalPoints) {
			winningTeam = key.getHomeTeam().toString();
		}else {
			winningTeam = key.getAwayTeam().toString();
		}
		System.out.println(String.format("%s\t%s(%d)\t%s(%d)\t%s", key.getDate(), key.getHomeTeam().toString(), homeTeamTotalPoints, key.getAwayTeam().toString(), awayTeamTotalPoints, winningTeam));
		Integer totalWins = teamWinningsMap.get(winningTeam);
		if (totalWins == null) {
			teamWinningsMap.put(winningTeam, Integer.valueOf(1));
		}else {
			teamWinningsMap.put(winningTeam, ++totalWins);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		int maxWins = Integer.MIN_VALUE;
		List<String> teams = new ArrayList<String>();
		for(String team : teamWinningsMap.keySet()) {
			Integer totalWins = teamWinningsMap.get(team);
			if (totalWins >= maxWins) {
				teams.add(team);
				maxWins = totalWins;
			}
		}
		for(String team : teams) {
			context.write(new Text(team), new IntWritable(maxWins));
		}
		super.cleanup(context);
	}

}

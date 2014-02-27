package com.sarungundram.hadoop.poc;

import com.sarungundram.hadoop.poc.entity.Play;

public class PlayParser {
	private static short PERIOD_INDEX = 10;
	private static short TEAM_INDEX = 12;
	private static short ETYPE_INDEX = 13;
	private static short NUM_INDEX = 20;
	private static short OUTOF_INDEX = 22;
	private static short PLAYER_INDEX = 23;
	private static short POINTS_INDEX = 24;
	private static short RESULT_INDEX = 27;

	public Play parse(String line) {
		Play play = new Play();
		String[] split = line.split(",");
		play.setPeriod(Integer.parseInt(split[PERIOD_INDEX]));
		play.setTeam(split[TEAM_INDEX]);
		play.setEtype(split[ETYPE_INDEX]);
		play.setNum(Integer.parseInt(split[NUM_INDEX]));
		play.setOutof(Integer.parseInt(split[OUTOF_INDEX]));
		play.setPlayer(split[PLAYER_INDEX]);
		play.setPoints(Integer.parseInt(split[POINTS_INDEX]));
		play.setResult(split[RESULT_INDEX]);
		
		return play;
	}
}

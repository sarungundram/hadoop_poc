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
	private static short TYPE_INDEX = 29;

	public Play parse(String line, long offset) {
		Play play = new Play();
		try {
			if (offset != 0 && !isBlank(line)) {
				String[] split = line.split(",");
				play.setPeriod(Integer.parseInt(split[PERIOD_INDEX]));
				String team = split[TEAM_INDEX];
				if (NUM_INDEX < split.length) {
					play.setTeam(team);
					play.setEtype(split[ETYPE_INDEX]);
					if (!isBlank(split[NUM_INDEX]))
						play.setNum(Integer.parseInt(split[NUM_INDEX]));
					if (!isBlank(split[OUTOF_INDEX]))
						play.setOutof(Integer.parseInt(split[OUTOF_INDEX]));
					play.setPlayer(split[PLAYER_INDEX]);
					if (!isBlank(split[POINTS_INDEX]))
						play.setPoints(Integer.parseInt(split[POINTS_INDEX]));
					if (RESULT_INDEX < split.length)
						play.setResult(split[RESULT_INDEX]);
					if (TYPE_INDEX < split.length)
						play.setType(split[TYPE_INDEX]);
				}
			}
		} catch (Throwable t) {
			System.err.println(offset + ":" + line);
			throw new RuntimeException(t);
		}
		return play;
	}

	private boolean isBlank(String input) {
		return input == null || input.trim().length() == 0;
	}

}

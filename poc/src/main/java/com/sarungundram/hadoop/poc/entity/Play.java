package com.sarungundram.hadoop.poc.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Play implements Writable {

	private IntWritable period = new IntWritable();
	
	private Text etype = new Text();
	
	private Text team = new Text();
	
	private Text player = new Text();
	
	private IntWritable points = new IntWritable();
	
	private IntWritable num = new IntWritable();
	
	private IntWritable outof = new IntWritable();
	
	private Text result = new Text();
	
	private Text type = new Text();
	
	
	public IntWritable getPeriod() {
		return period;
	}

	public Text getEtype() {
		return etype;
	}

	public Text getTeam() {
		return team;
	}

	public Text getPlayer() {
		return player;
	}

	public IntWritable getPoints() {
		return points;
	}

	public IntWritable getNum() {
		return num;
	}

	public IntWritable getOutof() {
		return outof;
	}

	public Text getResult() {
		return result;
	}

	public String getEtypeValue() {
		return etype.toString();
	}
	
	public int getNumValue() {
		return num.get();
	}

	public int getOutofValue() {
		return outof.get();
	}

	public int getPeriodValue() {
		return period.get();
	}

	public String getPlayerValue() {
		return player.toString();
	}

	public int getPointsValue() {
		return points.get();
	}

	public String getResultValue() {
		return result.toString();
	}

	public String getTeamValue() {
		return team.toString();
	}
	
	public String getTypeValue() {
		return type.toString();
	}

	public void readFields(DataInput in) throws IOException {
		period.readFields(in);
		etype.readFields(in);
		team.readFields(in);
		player.readFields(in);
		points.readFields(in);
		num.readFields(in);
		outof.readFields(in);
		result.readFields(in);
		type.readFields(in);
	}

	public void setEtype(String etype) {
		this.etype.set(etype);
	}

	public void setNum(int num) {
		this.num.set(num);
	}

	public void setOutof(int outof) {
		this.outof.set(outof);
	}

	public void setPeriod(int period) {
		this.period.set(period);
	}

	public void setPlayer(String player) {
		this.player.set(player);
	}

	public void setPoints(int points) {
		this.points.set(points);
	}

	public void setResult(String result) {
		this.result.set(result);
	}

	public void setTeam(String team) {
		this.team.set(team);
	}
	
	public void setType(String type) {
		this.type.set(type);
	}

	public void write(DataOutput out) throws IOException {
		period.write(out);
		etype.write(out);
		team.write(out);
		player.write(out);
		points.write(out);
		num.write(out);
		outof.write(out);
		result.write(out);
		type.write(out);
	}

	
	

	
}

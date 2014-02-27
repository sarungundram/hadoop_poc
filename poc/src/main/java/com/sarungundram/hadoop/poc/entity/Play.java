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
	
	public void write(DataOutput out) throws IOException {
		period.write(out);
		etype.write(out);
		team.write(out);
		player.write(out);
		points.write(out);
		num.write(out);
		outof.write(out);
		result.write(out);
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
	}

	public int getPeriod() {
		return period.get();
	}

	public void setPeriod(int period) {
		this.period.set(period);
	}

	public String getEtype() {
		return etype.toString();
	}

	public void setEtype(String etype) {
		this.etype.set(etype);
	}

	public String getTeam() {
		return team.toString();
	}

	public void setTeam(String team) {
		this.team.set(team);
	}

	public String getPlayer() {
		return player.toString();
	}

	public void setPlayer(String player) {
		this.player.set(player);
	}

	public int getPoints() {
		return points.get();
	}

	public void setPoints(int points) {
		this.points.set(points);
	}

	public int getNum() {
		return num.get();
	}

	public void setNum(int num) {
		this.num.set(num);
	}

	public int getOutof() {
		return outof.get();
	}

	public void setOutof(int outof) {
		this.outof.set(outof);
	}

	public String getResult() {
		return result.toString();
	}

	public void setResult(String result) {
		this.result.set(result);
	}

	
	

	
}

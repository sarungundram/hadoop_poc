package com.sarungundram.hadoop.poc.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PlayKey implements WritableComparable<PlayKey> {
	private Text date;
	
	private Text homeTeam;
	
	private Text awayTeam;
	
	private LongWritable offset;
	
	public PlayKey() {
		set(new Text(), new Text(), new Text(), new LongWritable());
	}
	
	public PlayKey(Text date, Text homeTeam, Text awayTeam, LongWritable offset) {
		set(date, homeTeam, awayTeam, offset);
	}
	
	public PlayKey(String date, String homeTeam, String awayTeam, long offset) {
		set (new Text(date), new Text(homeTeam), new Text(awayTeam), new LongWritable(offset));
	}
	
	public void set(Text date, Text homeTeam, Text awayTeam, LongWritable offset) {
		this.date = date;
		this.homeTeam = homeTeam;
		this.awayTeam = awayTeam;
		this.offset = offset;
	}
	
	public void write(DataOutput out) throws IOException {
		date.write(out);
		homeTeam.write(out);
		awayTeam.write(out);
		offset.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		date.readFields(in);
		homeTeam.readFields(in);
		awayTeam.readFields(in);
		offset.readFields(in);
	}
	
	public int hashCode() {
		return new HashCodeBuilder().append(date.hashCode())
			.append(homeTeam.hashCode()).append(awayTeam.hashCode())
			.append(offset.hashCode()).toHashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof PlayKey) {
			PlayKey other = (PlayKey) o;
			return new EqualsBuilder().append(date, other.date)
					.append(homeTeam, other.homeTeam)
					.append(awayTeam, other.awayTeam)
					.append(offset, other.offset)
					.isEquals();
		}
		return false;
	}
	
	public String toString() {
		return date + "\t" + homeTeam + "\t" + awayTeam + "\t";
	}

	public int compareTo(PlayKey o) {
		return new CompareToBuilder().append(homeTeam, o.homeTeam).append(awayTeam, o.awayTeam)
			.append(date, o.date).toComparison();
	}

	public Text getDate() {
		return date;
	}

	public Text getHomeTeam() {
		return homeTeam;
	}

	public Text getAwayTeam() {
		return awayTeam;
	}
}

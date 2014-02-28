package com.sarungundram.hadoop.poc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

public class PlayFileInputFormat extends FileInputFormat<PlayKey, Play> {

	@Override
	public RecordReader<PlayKey, Play> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new PlayFileRecordReader();
	}

	public static void main(String[] args) {
		Matcher matcher = PlayFileRecordReader.fileNamePattern
				.matcher("20081114ATLCHI.csv");
		if (matcher.matches()) {
			System.out.println(matcher.group(1) + "\t" + matcher.group(2)
					+ "\t" + matcher.group(3));
		}
	}

}

class PlayFileRecordReader extends RecordReader<PlayKey, Play> {
	private LineRecordReader reader = new LineRecordReader();
	private FileSplit fileSplit;

	private PlayKey key = null;
	private Play value = null;

	private String gameDate;
	private String homeTeam;
	private String awayTeam;
	
	private PlayParser playParser = new PlayParser();
	

	private static String fileNamePatternStr = "(\\d{8})([A-Z]{3})([A-Z]{3})\\.csv";

	static Pattern fileNamePattern = Pattern.compile(fileNamePatternStr);

	@Override
	public void close() throws IOException {
		reader.close();

	}

	@Override
	public PlayKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Play getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader.initialize(inputSplit, context);
		this.fileSplit = (FileSplit) inputSplit;

		parseInputFileName();
	}

	private void parseInputFileName() {
		Path path = fileSplit.getPath();
		Matcher matcher = fileNamePattern.matcher(path.getName());
		if (!matcher.matches()) {
			throw new RuntimeException("Input file name " + path.getName() + " does not match required pattern");
		}
		this.gameDate = matcher.group(1);
		this.awayTeam = matcher.group(2);
		this.homeTeam = matcher.group(3);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hasNext = reader.nextKeyValue();
		if (hasNext) {
			key = new PlayKey(gameDate, homeTeam, awayTeam, reader.getCurrentKey().get());
			value = playParser.parse( reader.getCurrentValue().toString());
			
		}
		return hasNext;
	}

}

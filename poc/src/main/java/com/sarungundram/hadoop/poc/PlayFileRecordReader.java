package com.sarungundram.hadoop.poc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import com.sarungundram.hadoop.poc.entity.Play;
import com.sarungundram.hadoop.poc.entity.PlayKey;

class PlayFileRecordReader extends RecordReader<PlayKey, Play> {
	private LineReader reader;
	private FileSplit fileSplit;

	private PlayKey key = null;
	private Play value = null;

	private String gameDate;
	private String homeTeam;
	private String awayTeam;

	private PlayParser playParser = new PlayParser();
	private Path path;
	private FileSystem fs;
	private long startOffset;
	private long end;
	private FSDataInputStream fileIn;
	private long pos;

	private static String fileNamePatternStr = "(\\d{8})\\.([A-Z]{3})([A-Z]{3})\\.csv";

	static Pattern fileNamePattern = Pattern.compile(fileNamePatternStr);

	public PlayFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
		this.path = split.getPath(index);
		this.fs = this.path.getFileSystem(context.getConfiguration());
		this.startOffset = split.getOffset(index);
		this.end = startOffset + split.getLength(index);

		fileIn = fs.open(path);
		reader = new LineReader(fileIn);
		this.pos = startOffset;

		parseInputFileName(this.path);
	}

	@Override
	public void close() throws IOException {
		reader.close();

	}

	@Override
	public float getProgress() throws IOException {
		if (startOffset == end) {
			return 0;
		}
		return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
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
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
	}

	private void parseInputFileName(Path path) {
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
		int newSize = 0;
		if (pos < end) {
			Text input = new Text();
			newSize = reader.readLine(input);
			value = playParser.parse(input.toString(), pos);
			key = new PlayKey(gameDate, homeTeam, awayTeam, pos);
			pos += newSize;
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}

	}

}
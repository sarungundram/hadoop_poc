import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class TimeFilter extends FilterFunc {
	private static String TIME_PATTERN_STR = "(\\d+):(\\d+)\\s+(a\\.m\\.|p\\.m\\.)";

	private static Pattern TIME_PATTERN = Pattern.compile(TIME_PATTERN_STR);

	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			String timeStr = (String) input.get(0);
			Matcher m = TIME_PATTERN.matcher(timeStr);
			if (m.matches()) {
				int hour = Integer.parseInt(m.group(1));
				int min = Integer.parseInt(m.group(2));
				boolean am = "a.m.".equals(m.group(3));
				boolean pm = !am;
				
				if ((hour >= 10 && hour <= 11 && pm) || 
						hour >= 12 && hour <=5 && am) {
					return Boolean.TRUE;
				}else {
					return Boolean.FALSE;
				}
			}
		} catch (ExecException ee) {
			throw new IOException("Caught exception processing input row ", ee);
		}
		return true;
	}

}
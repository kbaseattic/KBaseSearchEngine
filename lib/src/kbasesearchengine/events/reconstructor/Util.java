package kbasesearchengine.events.reconstructor;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

public class Util {
	public final static DateTimeFormatter DATE_PARSER =
			new DateTimeFormatterBuilder()
				.append(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"))
				.appendOptional(DateTimeFormat.forPattern(".SSS").getParser())
				.append(DateTimeFormat.forPattern("Z"))
				.toFormatter();	
	
	
	public final static DateTimeFormatter DATE_FORMATTER =
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZoneUTC();	
	
}

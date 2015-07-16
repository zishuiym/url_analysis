package com.datayes.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public final class DateTimeTools {

	private DateTimeTools(){}
	
	public static boolean isValidDateTimeStr(String dateTimeStr, String format)
	{
		boolean isValid = false;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
		try {
			simpleDateFormat.parse(dateTimeStr);
			isValid = true;
		} catch (ParseException e) {
			isValid = false;
		}
		return isValid;
	}
	
}

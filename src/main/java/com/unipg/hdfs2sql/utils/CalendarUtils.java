/**
 * 
 */
package com.unipg.hdfs2sql.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author maria
 *
 */
public class CalendarUtils {

  public static String dateFormat = "dd-MM-yyyy hh:mm";
  private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);

  public static String ConvertMilliSecondsToFormattedDate(long milliSeconds){
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(milliSeconds);
      return simpleDateFormat.format(calendar.getTime());
  }
}

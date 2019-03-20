//package io.odysz.semantic.util;
//
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.GregorianCalendar;
//import java.util.concurrent.TimeUnit;
//
//import io.odysz.common.dbtype;
//
///**Date formatting and parsing helper.<br>
// * This is basically used for string used in sql.
// * <br>For Json / Gson format helper, see {@link JDateFormat}.
// * @author ody */
//public class DateFormat {
//	/**yyyy-MM-dd or %Y-%M-%e*/
//	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//	/**yyyy-MM-dd-hhmmss or %Y-%M-%e ...*/
//	public static SimpleDateFormat sdflong_sqlite = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
//	public static SimpleDateFormat sdflong_mysql = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	/**yyyy-MM-dd
//	 * @param d
//	 * @return
//	 */
//	static public String format(Date d) { return d == null ? " - - " : sdf.format(d); }
////	static public String formatHms(Date d) { return d == null ? " - - " : sdflong.format(d); }
//	/**yyyy-MM-dd
//	 * @param text
//	 * @return
//	 * @throws ParseException
//	 */
//	public static Date parse(String text) throws ParseException { return sdf.parse(text); }
//
//	public static String incSeconds(dbtype drvType, String date0, int snds) throws ParseException {
//		Date d0 = parse(date0);
//		d0.setTime(d0.getTime() + snds);
//		// return format(d0);
//		if (drvType == dbtype.sqlite)
//			return sdflong_sqlite.format(d0);
//		return sdflong_mysql.format(d0);
//	}
//
//	/**yyyy年MM月dd日 */ 
////	public static final SimpleDateFormat sdfZh = new SimpleDateFormat("yyyy年MM月dd日");
//
//	/**https://stackoverflow.com/questions/9474121/i-want-to-get-year-month-day-etc-from-java-date-to-compare-with-gregorian-cal
//	 * @param date
//	 * @return
//	 */
//	public static String GetChineseYMD(Date date) {
//		if (date == null)
//			return "---- 年 -- 月 -- 日";
//		Calendar c = new GregorianCalendar();
//		c.setTime(date);
//	    int year = c.get(Calendar.YEAR);
//	    int month = c.get(Calendar.MONTH);
//	    int day = c.get(Calendar.DAY_OF_MONTH);
//	    return String.format("%1$4d年%2$02d月%3$02d日", year, month + 1, day);
//	}
//
//	public static String getDayDiff(Date date2, Date date1) {
//		if (date2 == null || date1 == null)
//			return "-";
//		return String.valueOf(getDayDiffInt(date2, date1));
//	}
//
//	public static long getDayDiffInt(Date d2, Date d1) {
//			if (d2 == null || d1 == null)
//			return -1;
//		long diff = d2.getTime() - d1.getTime();
//		return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
//	}
//
//	public static String getTimeStampYMDHms(dbtype drvType) {
//		Date now = new Date();
//		if (drvType == dbtype.sqlite)
//			return sdflong_sqlite.format(now);
//		return sdflong_mysql.format(now);
//	}
//}

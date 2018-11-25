package io.odysz.semantic.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import io.odysz.semantic.DA.Connects.DriverType;

/**Date formatting and parsing helper.<br>
 * This is basically used for datatime used in Json / Gson.
 * <p>For date format reference, see
 * <a href='https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html'>Class SimpleDateFormat API at Oracle</a><br>
 * For additional information of Json datetime format:
 * <a href='https://www.ibm.com/developerworks/library/j-javaee8-json-binding-4/index.html'>IBM Learn</a>
 * </p>
 * <br>For sql format helper, see {@link DateFormat}.
 * @author ody */
public class JDateFormat {
	/**yyyy-MM-dd*/
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static JDateFormat sdfInst;
	/**yyyy年MM月dd日 */ 
	private static final SimpleDateFormat sdfZh = new SimpleDateFormat("yyyy年MM月dd日");
	private static JDateFormat sdfZhInst;

	/**yyyy-MM-dd'T'HH:mm:ss.SSSZ, e.g. 2001-07-04T12:08:56.235-0700 */
	private static SimpleDateFormat iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	private static JDateFormat iso8601Inst;
	/**yyyy年MM月dd日'T'HH:mm:ss.SSSZ, e.g. 2001年07月04日T12:08:56.235-0700 */
	private static SimpleDateFormat iso8601zh = new SimpleDateFormat("yyyy年MM月dd日'T'HH:mm:ss.SSSZ");
	private static JDateFormat iso8601zhInst;
	
	/**yyyy-MM-dd HH:mm:ss, e.g. 2001-07-04 12:08:56.235 */
	private static SimpleDateFormat sdflong = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static JDateFormat sdflongInst;

	private SimpleDateFormat mysdf;
	public JDateFormat(SimpleDateFormat sdf) { mysdf = sdf; }

	/**yyyy年MM月dd日 */ 
	public static JDateFormat JdateZh() {
		if (sdfZhInst == null)
			sdfZhInst = new JDateFormat(sdfZh);
		return sdfZhInst;
	}

	/**yyyy-MM-dd*/
	public static JDateFormat Jdate() {
		if (sdfInst == null)
			sdfInst = new JDateFormat(sdf);
		return sdfInst;
	}

	/**yyyy年MM月dd日'T'HH:mm:ss.SSSZ, e.g. 2001年07月04日T12:08:56.235-0700 */
	public static JDateFormat iso8601Zh() {
		if (iso8601zhInst == null)
			iso8601zhInst = new JDateFormat(iso8601zh);
		return iso8601zhInst;
	}

	/**yyyy-MM-dd'T'HH:mm:ss.SSSZ, e.g. 2001-07-04T12:08:56.235-0700 */
	public static JDateFormat iso8601() {
		if (iso8601Inst == null)
			iso8601Inst = new JDateFormat(iso8601);
		return iso8601Inst;
	}

	/**yyyy-MM-dd HH:mm:ss, e.g. 2001-07-04 12:08:56.235 */
	public static JDateFormat simpleLong() {
		if (sdflongInst == null)
			sdflongInst = new JDateFormat(sdflong);
		return sdflongInst;
	}

	/**yyyy-MM-dd
	 * @param d
	 * @return
	 */
	public String format(Date d) { return d == null ? " - - " : mysdf.format(d); }

	public Date parse(String text) throws ParseException { return mysdf.parse(text); }

	public String incSeconds(DriverType drvType, String date0, int snds) throws ParseException {
		Date d0 = parse(date0);
		d0.setTime(d0.getTime() + snds);
		return format(d0);
	}

	public static String getDayDiff(Date date2, Date date1) {
		if (date2 == null || date1 == null)
			return "-";
		return String.valueOf(getDayDiffInt(date2, date1));
	}

	public static long getDayDiffInt(Date d2, Date d1) {
			if (d2 == null || d1 == null)
			return -1;
		long diff = d2.getTime() - d1.getTime();
		return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
	}

}

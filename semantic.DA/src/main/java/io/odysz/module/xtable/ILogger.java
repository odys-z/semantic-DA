package io.odysz.module.xtable;

/**@deprecated replaced by {@link io.odysz.common.Utils}<br>
 * Code snipet for default ILogger android implementation:<pre>
 public class Logger implements ILogger {
	private static boolean debugging = true;

	/ **All ILogger implementation instance use a static flag.
	 * It's recommended to set debug mode when the app initializing.
	 * (non-Javadoc)
	 * @see com.infochange.frame.xtable.ILogger#setDebugMode(boolean)
	 * /
	@ Override
	public void setDebugMode(boolean isDebug) {
		if (!debugging && isDebug)
			Log.w("Logger", "Toggling Logger from release mode to debug mode.");
		else
			Log.i("Logger", String.format("Setting Logger to debug mode = %s", isDebug));
		debugging = isDebug;
	}
	
	@ Override
	public void d(String tag, String line) { if (debugging) Log.d(tag, line); }

	@ Override
	public void i(String tag, String line) { if (debugging) Log.i(tag, line); }

	@ Override
	public void v(String tag, String line) { if (debugging) Log.v(tag, line); }

	@ Override
	public void e(String tag, String line) { Log.e(tag, line); }

	@ Override
	public void w(String tag, String line) { Log.w(tag, line); }
} </pre>
 * @author odys-z@github.com
 *
 */
public interface ILogger {
	/**Logger can working in debug mode and release mode.
	 * If in debug mode, i(), d(), v() are disabled.<br/>
	 * @param isDebug
	 * @return this logger
	 */
	ILogger setDebugMode(boolean isDebug);
	void e(String tag, String msg);
	void w(String tag, String msg);
	void i(String tag, String msg);
	void d(String tag, String msg);
	void v(String tag, String msg);
}

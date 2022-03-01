package io.odysz.module.xtable;

/**@deprecated replaced by {@link io.odysz.common.Utils}<br>
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

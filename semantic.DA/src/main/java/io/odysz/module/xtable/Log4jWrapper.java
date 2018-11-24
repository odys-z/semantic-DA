package io.odysz.module.xtable;

/**FIXME this class must been removed.
 * @author ody
 *
 */
public class Log4jWrapper implements ILogger {
	protected boolean debug;

	public Log4jWrapper(String name) {
		// log = Logger.getLogger(name);
	}

	@Override
	public ILogger setDebugMode(boolean isDebug) {
		debug= isDebug;
		return this;
	}

	@Override
	public void e(String tag, String msg) {
		// log.error(String.format(" %s - %s", tag, msg));
	}

	@Override
	public void w(String tag, String msg) {
		// log.warn(String.format(" %s - %s", tag, msg));
	}

	@Override
	public void i(String tag, String msg) {
//		if (debug)
//			log.info(String.format(" %s - %s", tag, msg));
	}

	@Override
	public void d(String tag, String msg) {
//		if (debug)
//			log.debug(String.format(" %s - %s", tag, msg));
	}

	@Override
	public void v(String tag, String msg) {
//		if (debug)
//			log.debug(String.format(" %s - %s", tag, msg));
	}

}

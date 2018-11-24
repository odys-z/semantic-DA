package io.odysz.semantic.DA;

public abstract class IrSingleton {
	public static final long version = 1L;
	public static final boolean debug = false;
	
	protected static String webRoot;
	public static String webRoot() { return webRoot; }
	public static void webRoot(String webRt) { webRoot = webRt; }

	protected static final String rootKey = "infochange-v2";
	
	
	/** System.err.println(String.format(templt, args));
	 * @param templt
	 * @param args
	 */
	public static void warn(String templt, Object ... args) {
		try {System.err.println(String.format(templt, args)); }
		catch (Exception e) { e.printStackTrace(); }
	}

	/**<pre>if (args == null) System.out.println(msg);
else System.out.println(String.format(msg, args));</pre>
	 * Call this with conditon if ({@link #debug}) logi(...) or something alike.<br>
	 * In this way java compiler will auto optimizing (remove) the unused code, leading empty operation of String.format(). 
	 * @param msg
	 */
	public static void logi(String msg, Object ... args) {
		if (args == null) System.out.println(msg);
		else 
			try { System.out.println(String.format(msg, args)); }
			catch (Exception e) { e.printStackTrace(); }

	}
}

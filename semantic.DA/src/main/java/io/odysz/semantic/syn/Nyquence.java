package io.odysz.semantic.syn;

public class Nyquence {

	/**
	 * 
	 * <pre>
	 * -128           0            127
	 *                a  b
	 *     a             b
	 *     b                    a             a - b &lt; 0
	 * </pre>
	 * @param a
	 * @param b
	 * @return 1 if a &gt; b else -1 if a &lt; b else 0
	 */
	public static int compare64(long a, long b) {
		long c = a - b;
		return c < 0 && c != Long.MIN_VALUE ? -1 : a == b ? 0 : 1;
	}

	public Long n;

	public Nyquence(long n) {
		this.n = n;
	}
}

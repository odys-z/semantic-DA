package io.odysz.semantic.syn;

public class Nyquence {

	/**
	 * Compare a, b within modal of max long.
	 * <pre>
	 * min-long<0        0            max-long
	 *                a  b
	 *     a             b
	 *     b                    a             a - b &lt; 0
	 * </pre>
	 * @param a
	 * @param b
	 * @return 1 if a &gt; b else -1 if a &lt; b else 0
	 */
	public static int compareNyq(long a, long b) {
		long c = a - b;
		return c < 0 && c != Long.MIN_VALUE ? -1 : a == b ? 0 : 1;
	}

	public Long n;

	public Nyquence(long n) {
		this.n = n;
	}

	public Nyquence inc(Nyquence maxn) {
		return inc(maxn.n);
	}

	Nyquence inc(long maxn) {
		this.n++;
		this.n = Math.max(maxn, this.n );
		return this;
	}

	public static long max(long a, long b) {
		return compareNyq(a, b) < 0 ? b : a;
	}

	public static long min(long a, long b) {
		return compareNyq(a, b) < 0 ? a : b;
	}

	public static Nyquence max(Nyquence a, Nyquence b) {
		return compareNyq(a.n, b.n) < 0 ? b : a;
	}

}

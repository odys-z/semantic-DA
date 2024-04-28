package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isNull;

import java.sql.SQLException;
import java.util.HashMap;

import io.odysz.module.rs.AnResultset;

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

	protected long n;

	public Nyquence(long n) {
		this.n = n;
	}

	public Nyquence(String n) {
		this(Long.valueOf(n));
	}

	public Nyquence inc(Nyquence... maxn) {
		if (isNull(maxn)) {
			++this.n;
			return this;
		}
		else
			return inc(maxn[0].n);
	}

	Nyquence inc(long maxn) {
		this.n++;
		this.n = Math.max(maxn, this.n );
		return this;
	}

	public static long maxn(long a, long b) {
		return compareNyq(a, b) < 0 ? b : a;
	}

	public static long minn(long a, long b) {
		return compareNyq(a, b) < 0 ? a : b;
	}

	public static Nyquence maxn(Nyquence a, Nyquence b) {
		return a == null ? b : b == null ? a : compareNyq(a.n, b.n) < 0 ? b : a;
	}

	public static int compareNyq(Nyquence a, Nyquence b) {
		return compareNyq(a.n, b.n);
	}

	public static Nyquence maxn(HashMap<String, Nyquence> nv) {
		Nyquence mx = null;
		for (Nyquence nyq : nv.values()) {
			mx = maxn(mx, nyq);
		}
		return mx;
	}

	public static Nyquence maxn(HashMap<String, Nyquence> nv, Nyquence n) {
		Nyquence mx = maxn(nv);
		return maxn(mx, n);
	}

	public static HashMap<String, Nyquence> clone(HashMap<String, Nyquence> from) {
		HashMap<String, Nyquence> nv = new HashMap<String, Nyquence>(from.size());
		for (String k : from.keySet())
			nv.put(k, new Nyquence(from.get(k).n));
		return nv;
	}

	public static Nyquence getn(AnResultset chal, String nyqcol) throws SQLException {
		return new Nyquence(chal.getString(nyqcol));
	}

	/**
	 * Get absolute distance.
	 * 
	 * @param a
	 * @param b
	 * @return | a - b |
	 */
	public static long abs(Nyquence a, Nyquence b) {
		return Math.abs(Math.min(a.n - b.n, b.n - a.n));
	}

}

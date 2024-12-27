package io.odysz.semantic.syn;

import static io.odysz.common.LangExt.isNull;
import static io.odysz.common.LangExt.isblank;

import java.sql.SQLException;
import java.util.HashMap;

import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.SynChangeMeta;
import io.odysz.transact.sql.parts.condition.ExprPart;

public class Nyquence {

	/**
	 * Compare a, b within modal of max long.
	 * <pre>
	 * min-long &lt; 0      0            max-long
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

	public long n;

	public Nyquence(long n) {
		this.n = n;
	}

	public Nyquence(String n) {
		this(Long.valueOf(n));
	}

	public Nyquence inc(Nyquence... maxn) {
		if (isNull(maxn)) {
			++this.n;    // FIXME
			return this;
		}
		else
			return inc(maxn[0].n);
	}

	/**
	 * Increase n, if less than {@code maxn}, set to {@code maxn}.
	 * 
	 * @param maxn
	 * @return this
	 */
	Nyquence inc(long maxn) {
		this.n = Math.max(maxn, this.n );
		this.n++;
		return this;
	}

	public Nyquence add(int inc) {
		n += inc;
		return this;
	}

	@Override
	public String toString() {
		return String.valueOf(n);
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

	public static Nyquence maxn(Nyquence a, Nyquence b, Nyquence... more) {
		Nyquence x = maxn(a, b);
		if (!isNull(more))
			for (Nyquence c : more)
				x = maxn(x, c);
		return x;
	}

	public static int compareNyq(Nyquence a, Nyquence b) {
		return compareNyq(a.n, b.n);
	}

	public static Nyquence maxn(HashMap<String, Nyquence> nv) {
		if (nv == null) return null;
		
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

	public static HashMap<String, Nyquence>[] clone(HashMap<String, Nyquence>[] nvs) {
		@SuppressWarnings("unchecked")
		HashMap<String, Nyquence>[] nv = (HashMap<String, Nyquence>[]) new HashMap<?, ?>[nvs.length];
		for (int ix = 0; ix < nvs.length; ix++)
			nv[ix] = clone(nvs[ix]);
		return nv;
	}

	/**
	 * Parse Nyquence from result set.
	 * @param chal
	 * @param nyqcol
	 * @return Nyquence
	 * @throws SQLException
	 */
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

	///////////////////////// sql helpers ///////////////////////////

	public static ExprPart sqlCompare(String alias, String nyqcol, Nyquence n) {
		return isblank(alias)
			? new ExprPart(String.format("%s - %d", nyqcol, n.n))
			: new ExprPart(String.format("%s.%s - %d", alias, nyqcol, n.n)); // FIXME
	}

	public static ExprPart sqlCompare(String lcol, String rcol) {
		return new ExprPart(String.format("%s - %s", lcol, rcol)); // FIXME
	}

	public static ExprPart sqlCompare(String lcol, long rcol) {
		return new ExprPart(String.format("%s - %s", lcol, rcol)); // FIXME
	}

	public static ExprPart sqlCompare(String lalias, String lcol, String ralias, String rcol) {
		return sqlCompare(String.format("%s.%s", lalias, lcol), String.format("%s.%s", ralias, rcol));
	}
	
	/**
	 * // FIXME Not used? Unfinished
	 * // FIXME Not used? Unfinished
	 * // FIXME Not used? Unfinished
	 * <pre>
	 * |uids  |nyquence|updcols|seq|
	 * +------+--------+-------+---+
	 * |X,0002|       2|       |  3|
	 * |X,0003|       2|       |  4|
	 * |X,0001|       2|       |  2|
	 * |Y,0003|       2|       |  1|
	 * |X,0004|       3|       |  2|</pre>
	 * @param m
	 * @return ??
	 */
	public static ExprPart sqlOrderby(SynChangeMeta m) { return null; }
}

package io.odysz.common;

/**Radix 64 (String) v.s. int converter
 * @version '=' is replaced by '-' for easyui compatibility (last '=' in id makes trouble).
 * @author ody
 *
 */
public class Radix64 {
	/**The same table as in db table ir_radix64 <br>
	 * Any modification must been synchronized.*/
	static char[] radchar = new char[] {
			'0', '1', '2', '3', '4', '5', '6', '7',		'8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
			'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',		'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
			'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd',		'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
			'm', 'n', 'o', 'p', 'q', 'r', 's', 't',		'u', 'v', 'w', 'x', 'y', 'z', '+', '-'};

	/**
	 * convert v to Radix64 integer. Chars are same to Base64 except '/', which is replaced by '-'
	 * @param v fix 6 bytes Base64 chars.
	 * @return
	 */
	public static String toString(int v) {
		char[] buf = new char[6];
		for (int i = 0; i < 6; i++) {
			int idx = v & 0x3f;
			char digit = radchar[idx];
			buf[5 - i] = digit;
			v = v >>> 6;
		}
		return String.valueOf(buf);
	}
	
	public static String toString(int v, int digits) {
		char[] buf = new char[digits];
		for (int i = 0; i < digits; i++) {
			int idx = v & 0x3f;
			char digit = radchar[idx];
			buf[digits - 1 - i] = digit;
			v = v >>> 6;
		}
		return String.valueOf(buf);
	
	}
}

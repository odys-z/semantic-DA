package io.oz.syn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class NyquenceTest {

	@Test
	void testCompare64() {
		long a = 1;
		long b = 2;
		
		assertTrue(Nyquence.compareNyq(a, b) < 0, "1 < 2");

		a = 0; b = 0;
		assertTrue(0 == Nyquence.compareNyq(a, b), "0 == 0");
		
		a = 0; b = Long.MAX_VALUE;
		assertTrue(Nyquence.compareNyq(a, b) < 0, " 0 < 2 ^ 63 - 1");

		a = 0; b = Long.MIN_VALUE;
		assertTrue(1 == Nyquence.compareNyq(a, b), "0 > - 2 ^ 63");

		a = 0; b = Long.MAX_VALUE - 1;
		assertEquals(-1, Nyquence.compareNyq(a, b));

		a = -1; b = Long.MAX_VALUE;
		assertEquals(1, Nyquence.compareNyq(a, b));

		a = Long.MAX_VALUE; b = Long.MAX_VALUE;
		assertEquals(0, Nyquence.compareNyq(a, b));

		a = Long.MIN_VALUE; b = Long.MIN_VALUE;
		assertEquals(0, Nyquence.compareNyq(a, b));

		a = Long.MIN_VALUE; b = Long.MIN_VALUE;
		assertEquals(0, Nyquence.compareNyq(a, b));

		a = Long.MAX_VALUE; b = Long.MIN_VALUE;
		assertEquals(-1, Nyquence.compareNyq(a, b));
		
		a = 2 ^ 62; b = - (2 ^ 62);
		assertEquals(1, Nyquence.compareNyq(a, b));
		
		// -2^2       -2^1       0      2^1      2^2-1
		//   -4         -2       0       2        3
		//              a                b               b-a == 4, defined as a > b

		// -2^63      -2^62      0      2^62     2^63-1
		//              a                b
		a = - (1L << 62); b = 1L << 62;
		// assertEquals(Long.MAX_VALUE+1, b - a);
		// assertEquals(Long.MIN_VALUE, b - a);
		assertTrue(Nyquence.compareNyq(a, b) > 0, "-2^62 > 2^62");

		a = - (1L << 62); b = (1L << 62) - 1;
		assertEquals(Long.MAX_VALUE, b - a);
		assertEquals(-1, Nyquence.compareNyq(a, b));  // b-1 == 3, a < b

		a = - (1L << 62); b = (1L << 62) + 1;
		assertEquals(Long.MIN_VALUE + 1, b - a);
		assertEquals(1, Nyquence.compareNyq(a, b));
	}

}

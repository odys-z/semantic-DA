package io.odysz.semantic.docsync;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.odysz.semantic.DBSync.Nyquence;

class NyquenceTest {

	@Test
	void testCompare64() {
		long a = 1;
		long b = 2;
		
		assertEquals(-1, Nyquence.compare64(a, b));

		a = 0; b = 0;
		assertEquals(0, Nyquence.compare64(a, b));
		
		a = 0; b = Long.MAX_VALUE;
		assertEquals(-1, Nyquence.compare64(a, b));

		a = 0; b = Long.MIN_VALUE;
		assertEquals(1, Nyquence.compare64(a, b));

		a = 0; b = Long.MAX_VALUE - 1;
		assertEquals(-1, Nyquence.compare64(a, b));

		a = -1; b = Long.MAX_VALUE;
		assertEquals(1, Nyquence.compare64(a, b));

		a = Long.MAX_VALUE; b = Long.MAX_VALUE;
		assertEquals(0, Nyquence.compare64(a, b));

		a = Long.MIN_VALUE; b = Long.MIN_VALUE;
		assertEquals(0, Nyquence.compare64(a, b));

		a = Long.MIN_VALUE; b = Long.MIN_VALUE;
		assertEquals(0, Nyquence.compare64(a, b));

		a = Long.MAX_VALUE; b = Long.MIN_VALUE;
		assertEquals(-1, Nyquence.compare64(a, b));
		
		a = 2 ^ 62; b = - (2 ^ 62);
		assertEquals(1, Nyquence.compare64(a, b));
		
		// -2^2       -2^1       0      2^1      2^2-1
		//   -4         -2       0       2        3
		//              a                b               b-a == 4, defined as a > b

		// -2^63      -2^62      0      2^62     2^63-1
		//              a                b
		a = - (1L << 62); b = 1L << 62;
		assertEquals(Long.MAX_VALUE+1, b - a);
		assertEquals(Long.MIN_VALUE, b - a);
		assertEquals(1, Nyquence.compare64(a, b));

		a = - (1L << 62); b = (1L << 62) - 1;
		assertEquals(Long.MAX_VALUE, b - a);
		assertEquals(-1, Nyquence.compare64(a, b));  // b-1 == 3, a < b

		a = - (1L << 62); b = (1L << 62) + 1;
		assertEquals(Long.MIN_VALUE + 1, b - a);
		assertEquals(1, Nyquence.compare64(a, b));
	}

}

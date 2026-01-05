package io.oz.syn;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

import io.odysz.common.Utils;

class SyndomContextTest {

	@Test
	void testPrint() {
		@SuppressWarnings("serial")
		HashMap<String, Nyquence> nv = new HashMap<String, Nyquence>() {
			{put("zsu-hub", new Nyquence(1989));}
			{put("zsu-1",   new Nyquence(89));}
			{put("zsu-2", new Nyquence(64));}
		};
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8.name())) {
			
			Utils.logOut(ps);
			SyndomContext.print("zsu-hub", nv, true);
			String fullOutput = baos.toString(StandardCharsets.UTF_8);
			System.out.print(fullOutput);
			assertEquals(
					"       |  zsu-1 |  zsu-2 | zsu-hub|\n" +
					"       +--------+--------+--------+\n" +
					"zsu-hub|     89 |     64 |   1989 |\n" +
					"       +--------+--------+--------+",
					fullOutput.stripTrailing());
		}
		catch (Exception e) { e.printStackTrace(); }
	}

}

package io.odysz.semantic.syn;

/**
 * Interface for separating Semantic.DA's utils, e. g. Docheck, from depending
 * on JUnit test package.
 * 
 * Implementation example:<pre>
 * import static io.odysz.common.LangExt.isNull;
 * import static org.junit.jupiter.api.Assertions.assertEquals;
 * import io.odysz.semantic.syn.IAssert;
 * public class AssertImpl implements IAssert {
 *   public <T> void equals(T a, T b, String... msg) throws Error {
 *     assertEquals(a, b, isNull(msg) ? null : msg[0]);
 *   }
 *   public void equals(int a, int b, String... msg) throws Error {
 *     assertEquals(a, b, isNull(msg) ? null : msg[0]);
 *   }
 *   public void fail(String e) throws Error {
 *     fail(e);
 *   }
 *   public void equals(long a, long b, String... msg) {
 *     ssertEquals(a, b, isNull(msg) ? null : msg[0]);
 *   }
 * }</pre>
 */
public interface IAssert {
	<T> void equals(T a, T b, String ...msg) throws Error;

	void equali(int a, int b, String ...msg) throws Error;

	void equall(long l, long m, String... msg);

	void fail(String format) throws Error;
}

package io.odysz.semantic;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.odysz.semantic.DASemantics.ShAutoKPrefix;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

class ShAutoKPrefixTest {

	@Test
	void testUnquote() throws TransException {
		assertEquals("abc", ShAutoKPrefix.unquote(new ExprPart("abc")));
		assertEquals("x", ShAutoKPrefix.unquote(new ExprPart("'x'")));
		assertEquals("x", ShAutoKPrefix.unquote(new ExprPart("'x'")));
		assertEquals("x", ShAutoKPrefix.unquote(Funcall.constVal("x")));
		assertEquals("x ", ShAutoKPrefix.unquote(Funcall.constVal("x ")));
		assertEquals("x", ShAutoKPrefix.unquote(ExprPart.constr("x")));
	}
}

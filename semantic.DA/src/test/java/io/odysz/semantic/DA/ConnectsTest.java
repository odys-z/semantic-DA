package io.odysz.semantic.DA;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.odysz.module.rs.SResultset;

class ConnectsTest {

	@BeforeAll
	void testInit() {
		File file = new File("src/test/res");
		String path = file.getAbsolutePath();
		System.out.println(path);
		Connects.init(path);
	}

	@Test
	void testGenId() {
	}

	@Test
	void testSelect() {
		/*
		insert into a_functions (flags, funcId, funcName, url, parentId, sibling, fullpath) values
		('test00', '0001', 'System Title', null, null, '0', '0 0001'),
		('test00', '0002', 'Portal', 'views/portal.html', '0001', '1', '0 0001.1 0002'),
		('test00', '0003', 'User Info', 'views/user-info.html', '0001', '2', '0 0001.2 0003');
		*/
		SResultset rs = Connects.select("select * from a_functions", Connects.flag_nothing);
	}

}

package io.odysz.semantic.util;

public class SQLString {

	public static String formatSql(String s) {
		if (s != null) {
			String s1 = s.replace("\n", "\\n");
			String s2 = s1.replace("\t", "\\t");
			String s3 = s2.replace("'", "''");
			return s3;
		}
		return "";
	}

}

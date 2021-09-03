package io.odysz.common;

import java.util.List;
import java.util.Map;

public class EnvHelper {

	/**
	 * Semantic-* will be used in the future mainly in docker container.
	 * In docker, volume can not mounted to tomcat/webapp's sub folder - will prevent war unpacking.
	 * See <a href='https://stackoverflow.com/q/15113700'>this problem</a>. 
	 * So it's necessary have file paths not only relative, but also can be parsed for replacing environment variables.
	 * @param src string have bash style variable to be replaced, e.g. $HOME/volume.sqlite
	 * @return string replaced with environment variables
	 */
	public static String replace(String src) {
		Regex reg = new Regex("\\$(\\w+)");
		List<String> envs = reg.findGroups(src);
		if (envs != null) {
			Map<String, String> sysenvs = System.getenv();

			for (String env : envs) {
				String v = sysenvs.get(env);
				src = src.replaceAll("\\$" + env, v == null ? System.getProperty(env) : v);
			}
		}
		return src;
	}
	
	/**A path start with "/" or "$" is absolute.
	 * @param path
	 * @return true if relative
	 */
	public static boolean isRelativePath(String path) {
		return !(path.startsWith("/") || path.startsWith("$"));
	}

}

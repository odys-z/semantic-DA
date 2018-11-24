package io.odysz.common;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**Regular Express helper. To match first letter 'P', set<br>
 * messages.xml/members.passport.regex = "^[pP]"<br>
 * and call:<br>
 * Regex.match(somestring);
 * See https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 * @author ody
 *
 */
public class Regex {
//	private static Pattern reg;
//	static {
//		try {
//			String regex = Configs.getCfg("members.passport.regex");
//			if (regex != null && regex.trim().length() > 0)
//				reg = Pattern.compile(regex);
//			else reg = null;
//		}catch (Exception ex) { ex.printStackTrace(); }
//	}
//
//	public static boolean staticMatch (String v) {
//		if (reg == null) return false;
//		Matcher matcher = reg.matcher(v);
//		return matcher.find();
//	}
	
	// also can be used in Instanced style
	private final Pattern regInst;
	public Regex(String pattern) {
		regInst = Pattern.compile(pattern);
	}
	
	public boolean match(String v) {
		Matcher matcher = regInst.matcher(v);
		return matcher.find();
	}

	public ArrayList<String> findGroups(String v) {
		Matcher matcher = regInst.matcher(v);
		ArrayList<String> vss = new ArrayList<String>(2) ;
        while (matcher.find()) {
        	vss.add(matcher.group());
        }
        return vss;
	}
	
	public ArrayList<String[]> findGroupsRecur(String v) {
        Matcher matcher = regInst.matcher(v);
        ArrayList<String[]> res = new ArrayList<String[]>();
        int i = 0;
        while (matcher.find()) {
//            System.out.println(" ---------------- ");
//            System.out.println(v);
//            System.out.print("Start index: " + matcher.start());
//            System.out.print(" End index: " + matcher.end() + " ");
//            System.out.println(matcher.group());

        	String[] grps = new String[matcher.groupCount()];
            for (int g = 0; g < matcher.groupCount(); g++) {
//            	System.out.println("group " + g + "\t" + matcher.group(g));
            	grps[g] = matcher.group(g);
            }
            res.add(grps);

            if (i++ > 20) break; // in case of wrong patter definition
            v = v.substring(matcher.end());
            matcher = regInst.matcher(v);
        }
	
        return res;
	}
	
	public int startAt(String v) {
        Matcher matcher = regInst.matcher(v);
        if (matcher.find())
        	return matcher.start();
        else return -1;
	}
}

package io.odysz.semantic;

import java.util.HashMap;

/**The equivalent of JsonObject in previous projects
 * @author ody
 *
 */
public class SemanticObject extends Object {

	private HashMap<String, Object> props;

	public SemanticObject get(String prop) {
		return props == null ? null : (SemanticObject) props.get(prop);
	}

	public String getString(String prop) {
		return props == null ? null : (String) props.get(prop);
	}

	public void put(String prop, String v) {
		if (props == null)
			props = new HashMap<String, Object>();
		props.put(prop, v);
	}

//	public void addObject(SemanticObject cond) {
//		// TODO Auto-generated method stub
//		
//	}

	public void put(String prop, SemanticObject obj) {
		if (props == null)
			props = new HashMap<String, Object>();
		props.put(prop, obj);
	}

}

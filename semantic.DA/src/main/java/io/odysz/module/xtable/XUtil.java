/**
 * 
 */
package io.odysz.module.xtable;

import java.lang.reflect.Constructor;

/** * @author odysseus.edu@gmail.com */
public class XUtil {
	/**Construct instance of class specified in classRecs,
	 * return object instantiated with constructorParas.<br/>
	 * For ex.:<br/>
	 * Object[] p = new Object[2];<br/>
	 * p[0] = res;<br/>
	 * p[1] = skinid;<br/>
	 * skin = (IMetronomeSkin) XUtil.getClassInstance(st, "class", p);<br/>
	 * @param classRecs xtable for class config. 
	 * @param classField class name field's name
	 * @param constructorParas paras for constructing new instance.
	 * @return new class instance
	 * @throws Exception
	 */
	static public Object getClassInstance(XMLTable classRecs, String classField, Object[] constructorParas) throws Exception {
		String clsname = classRecs.getString(classField);
		Class<?> cls = Class.forName(clsname);
		Class<?>[] constructorTypes;
		if (constructorParas != null) {
			constructorTypes = new Class[constructorParas.length];
			for (int i = 0; i < constructorParas.length; i++)
				constructorTypes[i] = constructorParas[i].getClass();
		}
		else
			constructorTypes = new Class[0];
//		Constructor<?> constor = cls.getConstructor(constructorTypes[0], constructorTypes[1], constructorTypes[2], constructorTypes[3], constructorTypes[4]);
//		return constor.newInstance(constructorParas);
		Constructor<?> contor =  cls.getConstructor(constructorTypes);
		return contor.newInstance(constructorParas);
	}
}

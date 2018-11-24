/**
 * 
 */
package io.odysz.semantic.DA;

import java.util.ArrayList;

import io.odysz.semantics.IrSingleton;

/**For any db update like synchronize task updating.
 * @author ody
 *
 */
public class DbLogDumb extends DbLog {
	
	@Override
	public void log(ArrayList<String> sqls) {
		if (IrSingleton.debug) {
			System.out.println("Ignored sqls by DbLogDumb.log():");
			DA.print(sqls);
		}
	}

}

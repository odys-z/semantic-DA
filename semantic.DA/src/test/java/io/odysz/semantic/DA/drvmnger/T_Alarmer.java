package io.odysz.semantic.DA.drvmnger;

import static io.odysz.common.LangExt.f;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;

import io.odysz.semantic.DATranscxt;
import io.odysz.semantics.IUser;
import io.odysz.semantics.SemanticObject;
import io.odysz.transact.sql.parts.condition.Funcall;
import io.odysz.transact.x.TransException;

public class T_Alarmer {

	private Thread t;
	private boolean[] lights;

	public T_Alarmer(DATranscxt st, IUser usr, String connId, int no, int max, String iv) {
		
		t = new Thread(() -> {try {
			for (int u = 0; u < max; u++) {
			  String aid = f("th %s - loop %s", no, u);
			  SemanticObject res = (SemanticObject) st
				.insert("b_alarms", usr)
				.nv("alarmId", aid)
				.nv("remarks", f("U %s.%s", no, u))
				.nv("typeId", iv)
				.post(st.delete("b_alarm_domain")
					.whereEq("aid", aid)
					.post(st.insert("b_alarm_domain")
						.cols("aid", "did")
						.select(st
						  .select("a_domain")
						  .col(Funcall.constr(aid))
						  .col("domainId")
						  .whereEq("domainValue", u % 4))))
				.ins(st.instancontxt(connId, usr))
				;
			  
			  assertEquals(1, res.total(0));
			}
			lights[no] = true;
		} catch (TransException | SQLException e) {
			e.printStackTrace();
		} }, "T_Alarmer");
	}

	public void start(boolean[] lights) {
		this.lights = lights;
		t.start();
	}

}

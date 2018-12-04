///**
// * 
// */
//package io.odysz.semantic.DA;
//
//import java.util.ArrayList;
//
//import io.odysz.common.Utils;
//import io.odysz.semantic.util.LogFlags;
//import io.odysz.transact.sql.Transcxt;
//
///**For any db update like synchronize task updating.
// * @author ody
// *
// */
//public class DbLogDumb extends DbLog {
//	
//
//	DbLogDumb(Transcxt st) {
//		super(st);
//	}
//
//	@Override
//	public void log(ArrayList<String> sqls) {
//		if (LogFlags.logDumb) {
//			Utils.logi("Ignored sqls by DbLogDumb.log():");
//			Utils.logi(sqls);
//		}
//	}
//
//}

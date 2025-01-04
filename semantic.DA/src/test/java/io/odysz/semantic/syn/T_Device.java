package io.odysz.semantic.syn;

import java.util.Date;

import io.odysz.common.DateFormat;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.transact.sql.Insert;

public class T_Device extends SynEntity {

	final String org;
	final String devname;

//	public T_Device(SyntityMeta entm) {
//		super(entm);
//		org = null;
//		devname = null;
//	}

	public T_Device(String synconn, String org, String devid, String devname) {
		super(new T_DA_DevMeta(synconn));
		this.org = org;
		this.recId = devid;
		this.devname = devname;
	}

	@Override
	public Insert insertEntity(SyntityMeta m, Insert ins) {
		T_DA_DevMeta md = (T_DA_DevMeta) m;
		return ins // .nv(md.domain, domain)
			.nv(md.pk, recId)
			.nv(md.device, recId)
			.nv(md.org, org)
			.nv(md.cdate, DateFormat.format(new Date()))
			.nv(md.devname, devname)
			.nv(md.owner, "Semantid.DA Junit")
			;
	}

}

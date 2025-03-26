package io.odysz.semantic.syn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.SQLException;
import java.util.Date;

import io.odysz.anson.AnsonField;
import io.odysz.common.DateFormat;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.meta.ExpDocTableMeta;
import io.odysz.semantic.meta.SyntityMeta;
import io.odysz.semantics.ISemantext;
import io.odysz.transact.sql.Insert;

import static io.odysz.common.LangExt.*;

/**
 * A sync object, server side and jprotocol oriented data record,
 * used for docsync.jserv. 
 * 
 * @author ody
 */
public class T_ExpSyncDoc extends SynEntity {
	protected static String[] synpageCols;

	public String recId;
	public String recId() { return recId; }
	public T_ExpSyncDoc recId(String did) {
		recId = did;
		return this;
	}

	public String pname;
	public String clientname() { return pname; }
	public T_ExpSyncDoc clientname(String clientname) {
		pname = clientname;
		return this;
	}

	public String clientpath;
	public String fullpath() { return clientpath; }

	public String mime() { return mime; }

	/** Non-public: doc' device id is managed by session. */
	protected String device;
	public String device() { return device; }
	public T_ExpSyncDoc device(String device) {
		this.device = device;
		return this;
	}
	
	public final String org;

	public String shareflag;
	public String shareflag() { return shareflag; }

	/** usally reported by client file system, overriden by exif date, if exits */
	public String createDate;
	public String cdate() { return createDate; }
	public T_ExpSyncDoc cdate(String cdate) {
		createDate = cdate;
		return this;
	}
	public T_ExpSyncDoc cdate(FileTime fd) {
		createDate = DateFormat.formatime(fd);
		return this;
	}

	public T_ExpSyncDoc cdate(Date date) {
		createDate = DateFormat.format(date);
		return this;
	}

	
	@AnsonField(shortenString=true)
	public String uri;
	public String uri() { return uri; }

	public String shareby;
	public String sharedate;
	
	public String syncFlag;

	/** usually ignored when sending request */
	public long size;

	public T_ExpSyncDoc shareby(String share) {
		this.shareby = share;
		return this;
	}

	public T_ExpSyncDoc sharedate(String format) {
		sharedate = format;
		return this;
	}

	public T_ExpSyncDoc sharedate(Date date) {
		return sharedate(DateFormat.format(date));
	}
	
	public T_ExpSyncDoc share(String shareby, String flag, String sharedate) {
		this.shareflag = flag;
		this.shareby = shareby;
		sharedate(sharedate);
		return this;
	}

	public T_ExpSyncDoc share(String shareby, String s, Date sharedate) {
		this.shareflag = s;
		this.shareby = shareby;
		sharedate(sharedate);
		return this;
	}

	@AnsonField(ignoreTo=true)
	ExpDocTableMeta docMeta;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	ISemantext semantxt;

	public String mime;
	
	public T_ExpSyncDoc(SyntityMeta m, String orgId) {
		super(m);
		org = orgId;
	}
	
	/**
	 * A helper used to make sure query fields are correct.
	 * @param meta
	 * @return cols for Select.cols()
	 */
	public static String[] nvCols(ExpDocTableMeta meta) {
		return new String[] {
				meta.pk,
				meta.resname,
				meta.uri,
				meta.createDate,
				meta.shareDate,
				meta.shareby,
				meta.shareflag,
				meta.mime,
				meta.fullpath,
				meta.device,
				meta.folder,
				meta.size
		};
	}
	
	/**
	 * @param meta
	 * @return String [meta.pk, meta.shareDate, meta.shareflag, meta.syncflag]
	 */
	public static String[] synPageCols(ExpDocTableMeta meta) {
		if (synpageCols == null)
			synpageCols = new String[] {
					meta.pk,
					meta.device,
					meta.fullpath,
					meta.shareby,
					meta.shareDate,
					meta.shareflag,
					meta.mime
			};
		return synpageCols;
	}

	public T_ExpSyncDoc(AnResultset rs, ExpDocTableMeta meta) throws SQLException {
		super(meta);
		this.docMeta = meta;
		this.recId = rs.getString(meta.pk);
		this.org = rs.getString(meta.org);
		this.pname = rs.getString(meta.resname);
		this.uri = rs.getString(meta.uri);
		this.createDate = rs.getString(meta.createDate);
		this.size = rs.getLong(meta.size, 0);
		
		this.clientpath =  rs.getString(meta.fullpath);
		this.device =  rs.getString(meta.device);
		this.folder = rs.getString(meta.folder);
		
		try {
			this.sharedate = DateFormat.formatime(rs.getDate(meta.shareDate));
		} catch (Exception ex) {
			this.sharedate = rs.getString(meta.createDate);
		}
		this.shareby = rs.getString(meta.shareby);
		this.shareflag = rs.getString(meta.shareflag);
		this.mime = rs.getString(meta.mime);
	}

	public T_ExpSyncDoc fullpath(String clientpath) throws IOException {
		this.clientpath = clientpath;

		if (isblank(createDate)) {
			try {
				Path p = Paths.get(clientpath);
				FileTime fd = (FileTime) Files.getAttribute(p, "creationTime");
				cdate(fd);
			}
			catch (IOException ex) {
				cdate(new Date());
			}
		}

		return this;
	}


	protected String folder;
	public String folder() { return folder; }
	public T_ExpSyncDoc folder(String v) {
		this.folder = v;
		return this;
	}

	/**
	 * Parse {@link PathsPage#clientPaths}.
	 * 
	 * @param flags
	 * @return this
	 */
	public T_ExpSyncDoc parseFlags(String[] flags) {
		if (!isNull(flags)) {
			syncFlag = flags[0];
			shareflag = flags[1];
			shareby = flags[2];
			sharedate(flags[3]);
		}
		return this;
	}
	
	@Override
	public Insert insertEntity(SyntityMeta m, Insert ins) {
		ExpDocTableMeta md = (ExpDocTableMeta) m;
		ins // .nv(md.domain, domain)
			.nv(md.folder, folder)
			.nv(md.org, org)
			.nv(md.mime, mime)
			.nv(md.uri, uri)
			.nv(md.size, size)
			.nv(md.createDate, createDate)
			.nv(md.resname, pname)
			.nv(md.device, device)
			.nv(md.shareby, shareby)
			.nv(md.shareDate, sharedate)
			.nv(md.shareflag, shareflag)
			.nv(md.fullpath, clientpath);
		return ins;
	}
}
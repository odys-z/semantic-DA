package io.odysz.semantic.syn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.attribute.FileTime;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.stream.Collectors;

import io.odysz.common.AESHelper;
import io.odysz.common.DateFormat;
import io.odysz.module.rs.AnResultset;
import io.odysz.semantic.syn.T_DocTableMeta.Share;
import io.odysz.transact.sql.parts.AbsPart;
import io.odysz.transact.sql.parts.condition.ExprPart;
import io.odysz.transact.sql.parts.condition.Funcall;

import static io.odysz.common.LangExt.isblank;
/**
 * Server side and jprotocol oriented data record - not BaseFile used by file picker (at Android client). 
 * 
 * @author ody
 *
 */
public class T_Photo extends T_SyncDoc {
	public String geox;
	public String geoy;
	
	/** usually ignored when sending request */
	public ArrayList<String> exif;
	public String exif() {
		return exif == null ? null
				: exif.stream()
				 .collect(Collectors.joining(","));
	}

	/** image size */
	public int[] widthHeight;
	/** reduction of image size */
	public int[] wh;
	
	/**
	 * Composed css json, saved as string.
	 * @see #css()
	 * */
	public String css;

	/**
	 * Compose a string representing json object for saving in DB.
	 * The type "io.oz.album.tier.PhotoCSS" doesn't exist at server side (v0.4.18)
	 * 
	 * @return string of json for saving
	 */
	public String css() {
		if (widthHeight != null)
			return String.format("{\"type\":\"io.oz.album.tier.PhotoCSS\", \"size\":[%s,%s,%s,%s]}",
				widthHeight[0], widthHeight[1], wh[0], wh[1]);
		else return "";
	}

	public String collectId;
	public String collectId() { return collectId; }

	public String albumId;
	
	public T_Photo() {}
	
	public T_Photo(AnResultset rs) throws SQLException {
		this.recId = rs.getString("pid");
		this.pname = rs.getString("pname");
		this.uri = rs.getString("uri");
		this.folder = rs.getString("folder");
		this.createDate = rs.getString("pdate");
		this.shareby = rs.getString("owner");
		this.geox = rs.getString("geox");
		this.geoy = rs.getString("geoy");
		
		this.css = rs.getString("css");
		
		// TODO debug
		this.clientpath =  rs.getString("clientpath");
		this.device =  rs.getString("device");
		
		try {
			this.sharedate = DateFormat.formatime(rs.getDate("sharedate"));
		} catch (SQLException ex) {
			this.sharedate = rs.getString("pdate");
		}
		this.geox = rs.getString("geox");
		this.geoy = rs.getString("geoy");
		
	}

	public T_Photo(String collectId, AnResultset rs) throws SQLException {
		this(rs);
		this.collectId = collectId;
	}

	/**Set client path and syncFlag
	 * @param rs
	 * @return this
	 * @throws SQLException
	 */
	public T_Photo asSyncRec(AnResultset rs) throws SQLException {
		this.clientpath = rs.getString("clientpath"); 
		this.syncFlag = rs.getString("syncFlag"); 
		return this;
	}

	public String folder() {
		if (folder == null)
			photoDate();
		return folder;
	}

	public AbsPart photoDate() {
		try {
			if (!isblank(createDate)) {
				Date d = DateFormat.parse(createDate); 
				folder = DateFormat.formatYYmm(d);
				return new ExprPart("'" + createDate + "'");
			}
			else {
				Date d = new Date();
				folder = DateFormat.formatYYmm(d);
				return Funcall.now();
			}
		} catch (ParseException e ) {
			e.printStackTrace();
			Date d = new Date();
			folder = DateFormat.formatYYmm(d);
			return Funcall.now();
		}
	}

	public void month(Date d) {
		folder = DateFormat.formatYYmm(d);
	}

	public void month(FileTime d) {
		folder = DateFormat.formatYYmm(d);
	}

	public void month(String d) {
		try {
			folder = DateFormat.formatYYmm(DateFormat.parse(d));
		} catch (ParseException e) {
			e.printStackTrace();
			folder = DateFormat.formatYYmm(new Date());
		}
	}

	public T_SyncDoc fullpath(String clientpath) throws IOException {
		super.fullpath(clientpath);

		if (isblank(folder)) {
			month(cdate());
		}
		return this;
	}

	public T_SyncDoc shareflag(String share) {
		shareflag = share;
		return this;
	}

	@SuppressWarnings("serial")
	public T_Photo create(String fullpath) throws IOException {
		File png = new File(fullpath);
		FileInputStream ifs = new FileInputStream(png);
		pname = png.getName();

		String b64 = AESHelper.encode64(ifs, 216); // 12 | 216, length = 219
		uri = b64;
		while (b64 != null) {
			b64 = AESHelper.encode64(ifs, 216); // FIXME this will padding useless bytes, what is happening when the file is saved at server side?
			if (b64 != null)
				uri += b64;
		}
		ifs.close();

		this.clientpath = fullpath;
		exif = new ArrayList<String>() {
			{add("location:вулиця Лаврська' 27' Київ");};
			{add("camera:Bayraktar TB2");}};
		share("ody@kyiv", Share.pub, new Date());

		return this;
	}

}
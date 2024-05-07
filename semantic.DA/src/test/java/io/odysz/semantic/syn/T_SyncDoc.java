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
import io.odysz.semantics.ISemantext;

import static io.odysz.common.LangExt.*;

/**
 * A sync object, server side and jprotocol oriented data record,
 * used for docsync.jserv. 
 * 
 * @author ody
 */
public class T_SyncDoc extends SynEntity {
	protected static String[] synpageCols;

	public String recId;
	public String recId() { return recId; }
	public T_SyncDoc recId(String did) {
		recId = did;
		return this;
	}

	public String pname;
	public String clientname() { return pname; }
	public T_SyncDoc clientname(String clientname) {
		pname = clientname;
		return this;
	}

	public String clientpath;
	public String fullpath() { return clientpath; }

	/** Non-public: doc' device id is managed by session. */
	protected String device;
	public String device() { return device; }
	public T_SyncDoc device(String device) {
		this.device = device;
		return this;
	}

	public String shareflag;
	public String shareflag() { return shareflag; }

	/** usally reported by client file system, overriden by exif date, if exits */
	public String createDate;
	public String cdate() { return createDate; }
	public T_SyncDoc cdate(String cdate) {
		createDate = cdate;
		return this;
	}
	public T_SyncDoc cdate(FileTime fd) {
		createDate = DateFormat.formatime(fd);
		return this;
	}

	public T_SyncDoc cdate(Date date) {
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

	public T_SyncDoc shareby(String share) {
		this.shareby = share;
		return this;
	}

	public T_SyncDoc sharedate(String format) {
		sharedate = format;
		return this;
	}

	public T_SyncDoc sharedate(Date date) {
		return sharedate(DateFormat.format(date));
	}
	
	public T_SyncDoc share(String shareby, String flag, String sharedate) {
		this.shareflag = flag;
		this.shareby = shareby;
		sharedate(sharedate);
		return this;
	}

	public T_SyncDoc share(String shareby, String s, Date sharedate) {
		this.shareflag = s;
		this.shareby = shareby;
		sharedate(sharedate);
		return this;
	}

	@AnsonField(ignoreTo=true)
	T_DocTableMeta docMeta;

	@AnsonField(ignoreTo=true, ignoreFrom=true)
	ISemantext semantxt;

	protected String mime;
	
	public T_SyncDoc() {super(null);}
	
	/**
	 * A helper used to make sure query fields are correct.
	 * @param meta
	 * @return cols for Select.cols()
	 */
	public static String[] nvCols(T_DocTableMeta meta) {
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
				meta.synoder,
				meta.folder,
				meta.size
		};
	}
	
	/**
	 * @param meta
	 * @return String [meta.pk, meta.shareDate, meta.shareflag, meta.syncflag]
	 */
	public static String[] synPageCols(T_DocTableMeta meta) {
		if (synpageCols == null)
			synpageCols = new String[] {
					meta.pk,
					meta.synoder,
					meta.fullpath,
					meta.shareby,
					meta.shareDate,
					meta.shareflag,
					meta.mime
			};
		return synpageCols;
	}

	public T_SyncDoc(AnResultset rs, T_DocTableMeta meta) throws SQLException {
		super(meta);
		this.docMeta = meta;
		this.recId = rs.getString(meta.pk);
		this.pname = rs.getString(meta.resname);
		this.uri = rs.getString(meta.uri);
		this.createDate = rs.getString(meta.createDate);
		this.size = rs.getLong(meta.size, 0);
		
		this.clientpath =  rs.getString(meta.fullpath);
		this.device =  rs.getString(meta.synoder);
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

	/**
	 * Load local file, take current time as sharing date.
	 * @param meta 
	 * @param fullpath
	 * @param owner
	 * @param shareflag
	 * @return this
	 * @throws IOException
	public SyncDoc loadFile(String fullpath, IUser owner, String shareflag) throws IOException {
		Path p = Paths.get(fullpath);
		byte[] f = Files.readAllBytes(p);
		String b64 = AESHelper.encode64(f);
		this.uri = b64;

		fullpath(fullpath);
		this.pname = p.getFileName().toString();
		
		this.shareby = owner.uid();
		this.shareflag = shareflag;
		sharedate(new Date());

		return this;
	}
	 */

//	public SyncDoc(IFileDescriptor d, String fullpath, DocTableMeta meta) throws IOException, SemanticException {
//		this.device = d.device();
//
//		this.docMeta = meta;
//		this.recId = d.recId();
//		this.pname = d.clientname();
//		this.uri = d.uri();
//		this.createDate = d.cdate();
//		this.mime = d.mime();
//		this.fullpath(fullpath);
//	}

	public T_SyncDoc fullpath(String clientpath) throws IOException {
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

	/**Set (private) jserv node file full path (path replaced with %VOLUME_HOME)
	 * @param path
	 * @return
	 * @throws SemanticException 
	 * @throws IOException 
	public IFileDescriptor uri(String path) throws SemanticException, IOException {
		fullpath(path);
		pname = FilenameUtils.getName(path);
		// throw new SemanticException("TODO");
		this.uri = null;
		return this;
	}
	 */

	protected String folder;
	public String folder() { return folder; }
	public T_SyncDoc folder(String v) {
		this.folder = v;
		return this;
	}

//	public SyncDoc parseMimeSize(String abspath) throws IOException {
//		mime = isblank(mime)
//				? Files.probeContentType(Paths.get(abspath))
//				: mime;
//
//		File f = new File(abspath);
//		size = f.length();
//		return this;
//	}

//	public SyncDoc parseChain(BlockChain chain) throws IOException {
//		createDate = chain.cdate;
//
//		device = chain.device;
//		clientpath = chain.clientpath;
//		pname = chain.clientname;
//		folder(chain.saveFolder);
//
//		shareby = chain.shareby;
//		sharedate = chain.shareDate;
//		shareflag = chain.shareflag;
//
//		return parseMimeSize(chain.outputPath);
//	}

	/**
	 * Parse {@link PathsPage#clientPaths}.
	 * 
	 * @param flags
	 * @return this
	 */
	public T_SyncDoc parseFlags(String[] flags) {
		if (!isNull(flags)) {
			syncFlag = flags[0];
			shareflag = flags[1];
			shareby = flags[2];
			sharedate(flags[3]);
		}
		return this;
	}
}
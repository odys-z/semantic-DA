package io.odysz.module.rs;


import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import io.odysz.anson.Anson;
import io.odysz.anson.AnsonField;
import io.odysz.anson.x.AnsonException;
import io.odysz.common.DateFormat;
import io.odysz.common.LangExt;
import io.odysz.common.Regex;
import io.odysz.transact.sql.parts.AnDbField;

/**
 * This Resultset is used for non-connected manipulation.
 * Rows and Cols are start at 1, the same as {@link java.sql.ResultSet}.<br>
 * TODO This will be changed in the future (It's proved starting at 0 is more bug free).
 * 
 * @author odys-z@github.com
 *
 */
public class AnResultset extends Anson {
	private static final boolean debug = true;

	/**
	 * Object creator for converting an entity record to a user type instance.
	 * 
	 * @author odys-z@github.com
	 *
	 * @param <T> the user type
	 * @since 1.4.12
	 */
	@FunctionalInterface
	public interface ObjCreator<T extends Anson> {
		T create(AnResultset rs) throws SQLException;
	}

	private int colCnt = 0;
	/**current row index, start at 1. */
	private int rowIdx = -1;
	/**
	 * current row index
	 * @since 1.4.25
	 * @return
	 */
	public int currentRow() { return rowIdx; }
	private int rowCnt = 0;

	// TODO docs
	@AnsonField(valType="java.util.ArrayList")
	private ArrayList<ArrayList<Object>> results;

	/**col-index start at 1, map: [alais(upper-case), [col-index, db-col-name(in raw case)]<br>
	 * case 1<pre>
	String colName = rsMeta.getColumnLabel(i).toUpperCase();
	colnames.put(colName, new Object[] {i, rsMeta.getColumnLabel(i)});
		</pre>
		case 2<pre>
for (String coln : colnames.keySet()) 
	colnames.put(coln.toUpperCase(), new Object[] {colnames.get(coln), coln});
		</pre>
	 * */
	@AnsonField(valType="[Ljava.lang.Object;")
	private HashMap<String, Object[]> colnames;
	public HashMap<String, Object[]> colnames() { return colnames; }

	/** In cs version, it's Datatable */
	@AnsonField(ignoreTo = true)
	private ResultSet rs;

	@AnsonField(ignoreTo = true, ignoreFrom = true)
	private Connection conn;

	@AnsonField(ignoreTo = true, ignoreFrom = true)
	private Statement stmt;

	/**For paged query, this the total row count*/
	private int total = 0;

	private HashMap<Class<?>,String> stringFormats;

	/** for deserializing */
	public AnResultset() { }

	public AnResultset(ResultSet rs) throws SQLException {
		ICRconstructor(rs);
	}

	public AnResultset(ResultSet rs, Connection connection, Statement statement) throws SQLException {
		this.rs = rs;
		conn = connection;
		stmt = statement;
		ICRconstructor(rs);
		this.rs.beforeFirst();
	}

	public void ICRconstructor(ResultSet rs) throws SQLException {
		results = new ArrayList<ArrayList<Object>>();
		if (rs == null) return;
		ResultSetMetaData rsMeta = rs.getMetaData();
		colCnt = rsMeta.getColumnCount();
		colnames = new HashMap<String, Object[]>();
		for (int i = colCnt; i >= 1; i--) {
			// 2017-11-25, in mysql testing, getColumnName returning original db name, rsMeta.getColumnLabel() returning alias.
			// String colName = rsMeta.getColumnName(i).toUpperCase();
			String colName = rsMeta.getColumnLabel(i).toUpperCase();
			if (colnames.containsKey(colName)) {
				if (debug)
					System.err.println("WARN: As duplicated col name been found, only the last one's index is reserved: " + colName);
			}
			colnames.put(colName, new Object[] {i, rsMeta.getColumnLabel(i)});
		}
		rowIdx = 0;
		rowCnt = 0;
		while (rs.next()) {
			rowCnt++;
			ArrayList<Object> row = new ArrayList<Object>();
			for (int j = 1; j <= colCnt; j++) {
				row.add(rs.getObject(j));
			}
			results.add(row);
		}
	}

	public AnResultset(AnResultset icrs) throws SQLException {
		results = new ArrayList<ArrayList<Object>>();
		if (icrs == null) return;
		HashMap<String, Object[]> src_colnames = icrs.getColnames();
		colnames = new HashMap<String, Object[]>();
		for (String cname : src_colnames.keySet()) {
			Object[] v = src_colnames.get(cname);
			colnames.put(cname, new Object[] {v[0], new String((String) v[1])});
		}
		rowIdx = 0;
		rowCnt = 0;
		colCnt = src_colnames.keySet().size();
		while (icrs.next()) {
			rowCnt++;
			ArrayList<Object> row = new ArrayList<Object>();
			for (int j = 1; j <= colCnt; j++) {
				String v = icrs.getString(j);
				if (v == null) row.add("");
				else row.add(new String(v));
			}
			results.add(row);
		}
	}
	
	/**Construct an empty set, used for appending rows.
	 * @param colnames
	 * @param rows 
	 */
	@SafeVarargs
	public AnResultset(HashMap<String, Integer> colnames, ArrayList<ArrayList<Object>>... rows) {
		results = new ArrayList<ArrayList<Object>>();
		colCnt = colnames.size();
		this.colnames = new HashMap<String, Object[]>(colCnt);
		for (String coln : colnames.keySet()) {
			this.colnames.put(coln.toUpperCase(), new Object[] {colnames.get(coln), coln});
		}
		rowIdx = 0;
		rowCnt = 0;
		
		if (rows != null && rows[0] != null) {
			results = rows[0];
			rowIdx = 0;
			rowCnt = results.size();
		}
	}

//	public AnResultset(HashMap<String, Object[]> columns, ArrayList<ArrayList<Object>> rows) {
//		results = new ArrayList<ArrayList<Object>>();
//		colCnt = colnames.size();
//		this.colnames = new HashMap<String, Object[]>(colCnt);
//		for (String coln : colnames.keySet()) {
//			this.colnames.put(coln.toUpperCase(), new Object[] {colnames.get(coln), coln});
//		}
//		rowIdx = 0;
//		rowCnt = 0;
//		
//		if (rows != null && rows[0] != null) {
//			results = rows[0];
//			rowIdx = 0;
//			rowCnt = results.size();
//		}
//	}


	/**Construct an empty set, used for appending rows.
	 * Cols are deep copied.
	 * @param colnames
	 */
	public AnResultset(HashMap<String, Object[]> colnames, boolean toUpperCase) {
		results = new ArrayList<ArrayList<Object>>();
		colCnt = colnames.size();
		this.colnames = new HashMap<String, Object[]>(colCnt);
		for (String coln : colnames.keySet()) {
			this.colnames.put(toUpperCase ? coln.toUpperCase() : coln, colnames.get(coln));
		}
		rowIdx = 0;
		rowCnt = 0;
	}

	/**Append a new row - deep copy, set current row as the appended row.
	 * @param row
	 * @return this
	 */
	public AnResultset appendDeeply(ArrayList<Object> row) {
		ArrayList<Object> newRow = new ArrayList<Object>(row.size());
		for (int j = 0; j < row.size(); j++) {
			String v = "";
			try { v = row.get(j).toString(); }
			catch (Exception e) {}
			newRow.add(v);
		}
		results.add(newRow);
		rowCnt++;
		rowIdx = results.size();
		return this;
	}
	
	public AnResultset append(ArrayList<Object> includingRow) {
		results.add(includingRow);
		rowCnt++;
		rowIdx = results.size();
		return this;
	}

	/**For debug, generate a fake rs.
	 * @param rows
	 * @param cols
	 */
	public AnResultset(int rows, int cols, String colPrefix) {
		if (rows <= 0 || cols <= 0)
			return;
		results = new ArrayList<ArrayList<Object>>(rows);
		colCnt = cols;
		colnames = new HashMap<String, Object[]>(cols);
		for (int i = colCnt; i >= 1; i--) {
			String colName = colPrefix == null  ? String.valueOf(i) : String.format("%s%s", colPrefix.trim(), i);
			colnames.put(colName.toUpperCase(), new Object[] {i, colName});
		}
		rowIdx = 0;
		rowCnt = 0;
		for (int r = 0; r < rows; r++) {
			rowCnt++;
			ArrayList<Object> row = new ArrayList<Object>(colCnt);
			for (int j = 1; j <= colCnt; j++) {
				row.add(String.format("%s, %s", r, j));
			}
			results.add(row);
		}
	}
	
	public AnResultset(int rows, int cols) throws Exception {
		if (rows <= 0 || cols <= 0)
			return;

		results = new ArrayList<ArrayList<Object>>(rows);
		colCnt = cols;
		colnames = new HashMap<String, Object[]>(cols);

		for (int i = colCnt; i >= 1; i--) {
			String colName = String.format("c-%1$s", i);
			colnames.put(colName.toUpperCase(), new Object[] {i, colName});
		}
		rowIdx = 0;
		rowCnt = 0;
		for (int r = 0; r < rows; r++) {
			rowCnt++;
			ArrayList<Object> row = new ArrayList<Object>(colCnt);
			for (int j = 0; j < colCnt; j++) {
				row.add(String.valueOf(r * cols + j));
			}
			results.add(row);
		}
	}
	
	public AnResultset(int rows, String[] colNames, boolean generateData) {
		if (rows <= 0 || colNames == null  || colNames.length == 0)
			return;
		results = new ArrayList<ArrayList<Object>>(rows);
		colCnt = colNames.length;
		this.colnames = new HashMap<String, Object[]>(colCnt);
		for (int i = colCnt; i >= 1; i--) {
			String cn = colNames[i - 1] == null ? String.valueOf(i): colNames[i - 1];
			colnames.put(cn.toUpperCase(), new Object[] {i, cn});
		}
		rowIdx = 0;
		rowCnt = 0;
		for (int r = 0; r < rows; r++) {
			rowCnt++;
			ArrayList<Object> row = new ArrayList<Object>(colCnt);
			for (int j = 1; j <= colCnt; j++) {
				if (generateData)
					row.add(String.format("%s, %s", r, j));
				else row.add("");
			}
			results.add(row);
		}
	}

	public AnResultset(String[] colNames) {
		results = new ArrayList<ArrayList<Object>>(16);
		colCnt = colNames.length;
		this.colnames = new HashMap<String, Object[]>(colCnt);
		for (int i = colCnt; i >= 1; i--) {
			// colnames.put(colNames[i - 1] == null ? String.valueOf(i): colNames[i - 1].toUpperCase(), i);
			String cn = colNames[i - 1] == null ? String.valueOf(i): colNames[i - 1];
			colnames.put(cn.toUpperCase(), new Object[] {i, cn});
		}
		rowIdx = 0;
		rowCnt = 0;
	}

	public AnResultset(ArrayList<String> colNames) {
		results = new ArrayList<ArrayList<Object>>(16);
		colCnt = colNames.size();
		this.colnames = new HashMap<String, Object[]>(colCnt);
		for (int i = colCnt; i >= 1; i--) {
			// colnames.put(colNames.get(i - 1) == null ? String.valueOf(i): colNames.get(i - 1).toUpperCase(), i);
			String cn = colNames.get(i - 1) == null ? String.valueOf(i): colNames.get(i - 1);
			colnames.put(cn.toUpperCase(), new Object[] {i, cn});
		}
		rowIdx = 0;
		rowCnt = 0;
	}

	public AnResultset results(ArrayList<ArrayList<Object>> rows) {
		this.results = rows;
		/*
		colCnt = colNames.size();
		this.colnames = new HashMap<String, Object[]>(colCnt);
		for (int i = colCnt; i >= 1; i--) {
			// colnames.put(colNames.get(i - 1) == null ? String.valueOf(i): colNames.get(i - 1).toUpperCase(), i);
			String cn = colNames.get(i - 1) == null ? String.valueOf(i): colNames.get(i - 1);
			colnames.put(cn.toUpperCase(), new Object[] {i, cn});
		}
		*/
		rowIdx = 0;
		rowCnt = rows.size();

		return this;
	}

	/** @return column names */
	public HashMap<String, Object[]> getColnames() {
		return colnames;
	}

	public boolean hasCol(String c) {
		return LangExt.isblank(c) ? false
				: getColnames().containsKey(c.trim().toUpperCase());
	}
	
	public ArrayList<ArrayList<Object>> getRows() {
		return results;
	}

	public boolean next() throws SQLException {
		rowIdx++;
		if (rs != null) rs.next();
		if (rowIdx > rowCnt) return false;
		else return true;
	}
	
	/**last start at 1, included in nexting range.<br>
	 * If current index = 4, nextUntill(5) return true;<br>
	 * If current index = 5, nextUntill(5) return false;
	 * @param last
	 * @return true: ok
	 * @throws SQLException
	 */
	public boolean nextUntill(int last) throws SQLException {
		rowIdx++;
		if (rs != null) rs.next();
		if (rowIdx > rowCnt || rowIdx > last) return false;
		else return true;
	}
	
	public int append(AnResultset more) throws SQLException {
		// check cols
		if (colCnt != more.getColCount()) throw new SQLException("Columns not matched.");
		results.addAll(((AnResultset)more).results);
		rowCnt += ((AnResultset)more).rowCnt;
		return rowCnt;
	}
	
	/**Add a formatter to type of clz when converting to String.
	 * @param clz
	 * @param format
	 * @return this
	 */
	public AnResultset stringFormat(Class<?> clz, String format) {
		if (stringFormats == null)
			stringFormats = new HashMap<Class<?>, String>();
		stringFormats.put(clz, format);
		return this;
	}
	
	public String getString(int colIndex) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) return null;
			if (results.get(rowIdx - 1).get(colIndex - 1) == null) return null;
			else {
				Object v = results.get(rowIdx - 1).get(colIndex - 1);
				return stringFormats != null && stringFormats.containsKey(v.getClass()) ?
						String.format(stringFormats.get(v.getClass()), v) : v.toString();
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage() + " Empty Results?");
		}
	}
	
	public String getString(String colName) throws SQLException {
		if (colName == null) return null;
		return getString((Integer) (colnames.get(colName.toUpperCase())[0]));
	}
	
	/**If field is a date value, return string formatted by sdf.
	 * @param colName
	 * @param sdf
	 * @return string value
	 * @throws SQLException
	 */
	public String getString(String colName, SimpleDateFormat sdf) throws SQLException {
		if (colName == null) return null;
		return getString((Integer)colnames.get(colName.toUpperCase())[0], sdf);
	}
	
	/**If field is a date value, return string formatted by sdf.
	 * @param colIndex
	 * @param sdf
	 * @return string value
	 * @throws SQLException
	 */
	public String getString(int colIndex, SimpleDateFormat sdf) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) return null;
			if (results.get(rowIdx - 1).get(colIndex - 1) == null) return null;
			else {
				Object obj = results.get(rowIdx - 1).get(colIndex - 1);
				if (obj instanceof Date)
					return sdf.format(obj);

				// return results.get(rowIdx - 1).get(colIndex - 1).toString();
				Object v = results.get(rowIdx - 1).get(colIndex - 1);
				return stringFormats != null && stringFormats.containsKey(v.getClass()) ?
						String.format(stringFormats.get(v.getClass()), v) : v.toString();
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	/**
	 * if null, change to ""
	 * @param colName
	 * @return string value
	 * @throws SQLException
	 */
	public String getStringNonull(String colName) throws SQLException {
		if (colName == null) return "";
		String s = getString((Integer)colnames.get(colName.toUpperCase())[0]);
		return s == null? "" : s;
	}

	public String getStringAtRow(String colName, int row) throws NumberFormatException, SQLException {
		return getStringAtRow(getColumex(colName), row);
	}

	public String getStringAtRow(int col, int row) throws NumberFormatException, SQLException {
		return String.valueOf(getRowAt(row - 1).get(col));
	}


	/**
	 * if value is equals case insensitive to 1,true, yes, y, t, decimal &gt; 0.001 return true, else return false;
	 * @param colIndex
	 * @return string value
	 * @throws SQLException
	 */
	public boolean getBoolean(int colIndex) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) return false;
			if (results.get(rowIdx - 1).get(colIndex - 1) == null) return false;
			else {
				try {
					String v = String.valueOf(results.get(rowIdx - 1).get(colIndex - 1)).trim();
					if (v == null) return false;
					v = v.toLowerCase();

					if (v.equals("1")) return true;
					if (v.equals("true")) return true;
					if (v.equals("yes")) return true;
					if (v.equals("y")) return true;
					if (v.equals("t")) return true;
					try {
						Double d = Double.valueOf(v);
						if (d >= 0.001d) return true;
					}
					catch (Exception e){}
					return false;
				} catch (Exception e) {
					return false;
				}
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public boolean getBoolean(String colName) throws SQLException {
		return getBoolean((Integer)colnames.get(colName.toUpperCase())[0]);
	}

	public double getDouble(int colIndex) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null)
				throw new SQLException("Null row to be accessed.");

			if (results.get(rowIdx - 1).get(colIndex - 1) == null)
				throw new SQLException("Null value to be converted to double.");

			else return Double.valueOf(results.get(rowIdx - 1).get(colIndex - 1).toString());
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public double getDouble(String colName) throws SQLException {
		return getDouble((Integer)colnames.get(colName.toUpperCase())[0]);
	}

//	public BigDecimal getBigDecimal(int colIndex) throws SQLException {
//		return BigDecimal.valueOf(getDouble(colIndex));
//	}
//
//	public BigDecimal getBigDecimal(String colName) throws SQLException {
//		return BigDecimal.valueOf(getDouble((Integer)colnames.get(colName.toUpperCase())[0]));
//	}
	
	public Date getDate(int index)throws SQLException{
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null)
				throw new SQLException("Null row to be accessed.");

			if (results.get(rowIdx - 1).get(index - 1) == null) return null;
			// Oracle Datetime, Mysql Date, datetime can be safely casted to date.
			// If your debugging arrived here, you may first check you database column type.
			else try {
				return (Date)results.get(rowIdx - 1).get(index - 1);
			} catch (ClassCastException e) {
				// tolerate text date
				return DateFormat.parse((String)results.get(rowIdx - 1).get(index - 1));
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public Date getDate(String colName)throws SQLException{
		return getDate((Integer)colnames.get(colName.toUpperCase())[0]);
	}

	/**
	 * @param index
	 * @return datetime
	 * @throws SQLException
	 */
	public Date getDateTime(int index)throws SQLException{
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null)
				throw new SQLException("Null row to be accessed.");

			if (results.get(rowIdx - 1).get(index - 1) == null) return null;
			// Oracle Datetime, Mysql Date, datetime can safely cast to date.
			else try {
				return (Date)results.get(rowIdx - 1).get(index - 1);
			} catch (ClassCastException e) {
				return DateFormat.parseDateTime((String)results.get(rowIdx - 1).get(index - 1));
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public Date getDateTime(String colName)throws SQLException{
		return getDateTime((Integer)colnames.get(colName.toUpperCase())[0]);
	}

	public int getInt(int colIndex) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) throw new SQLException("Null row to be accessed.");
			if (results.get(rowIdx - 1).get(colIndex - 1) == null) throw new SQLException("Null value to be converted to int.");
			else return Integer.valueOf(results.get(rowIdx - 1).get(colIndex - 1).toString());
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	public int getInt(String col, int deflt) {
		try {
			return getInt(col);
		} catch (SQLException e) {
			return deflt;
		}
	}
	
	public long getLong(int colIndex) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null)
				throw new SQLException("Null row to be accessed.");
			if (results.get(rowIdx - 1).get(colIndex - 1) == null)
				throw new SQLException("Null value to be converted to long.");
			else
				return Long.valueOf(results.get(rowIdx - 1).get(colIndex - 1).toString());
		}
		catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}

	public long getLong(String size, long empty) {
		try {return getLong(size);}
		catch (Exception e) { return empty; }
	}

	public long getLong(String colName) throws SQLException {
		return getLong((Integer)colnames.get(colName.toUpperCase())[0]);
	}
	
	/**
	 * @param colName
	 * @param row0 index start at 0
	 * @return v
	 * @throws NumberFormatException
	 * @throws SQLException
	 */
	public long getLongAt(String colName, int row) throws NumberFormatException, SQLException {
		return getLongAtRow(getColumex(colName) - 1, row);
	}

	/**
	 * @param col column index start at 0
	 * @param row0 index start at 0
	 * @return v
	 * @throws NumberFormatException
	 * @throws SQLException
	 */
	public long getLongAtRow(int col, int row0) throws NumberFormatException, SQLException {
		return Long.valueOf(String.valueOf(getRowAt(row0).get(col)));
	}

	public int getInt(String colName) throws SQLException {
		return getInt((Integer)colnames.get(colName.toUpperCase())[0]);
	}

	public Blob getBlob(int colIndex) throws SQLException {
		try {
			if (rs == null) throw new SQLException("Can not get Blob constructed by OracleHelper.select(). To access Blob, use OracleHelper.selectBlob()");
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) throw new SQLException("Null row to be accessed.");
			if (results.get(rowIdx - 1).get(colIndex - 1) == null) throw new SQLException("Null value to be converted to blob.");
			else return (Blob)rs.getBlob(colIndex);
		}catch (Exception e) {throw new SQLException(e.getMessage());}
	}
	
	public Blob getBlob(String colName) throws SQLException {
		return getBlob((Integer)colnames.get(colName.toUpperCase())[0]);
	}

	public Object getObject(int colIndex) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) throw new SQLException("Null row to be accessed.");
			//if (results.get(rowIdx - 1).get(colIndex - 1) == null) throw new SQLException("Null value to be converted to object.");
			else return results.get(rowIdx - 1).get(colIndex - 1);
		}catch (Exception e) {throw new SQLException(e.getMessage());}
	}
	
	/**
	 * @since 1.4.25
	 * @param <T>
	 * @param colIndex
	 * @return Anson instance (value unescaped)
	 * @throws AnsonException
	 * @throws SQLException
	 * @since 1.4.27
	 */
	@SuppressWarnings("unchecked")
	public <T extends AnDbField> T getAnson(int colIndex) throws AnsonException, SQLException {
		return (T) Anson.fromJson(getString(colIndex));
	}

	/**
	 * @param col
	 * @return Anson instance (value unescaped)
	 * @throws AnsonException
	 * @throws SQLException
	 * @since 1.4.27
	 */
	@SuppressWarnings("unchecked")
	public <T extends AnDbField> T getAnson(String col) throws AnsonException, SQLException {
		return (T) Anson.fromJson(getString(col));
	}
	
	/**
	 * Get current row index.<br>
	 * Row index start from 1.<br>
	 * The last row indix == getRowCount()
	 * @return row index
	 */
	public int getRow() {
		if (results == null) return 0;
		return rowIdx;
	}
	
	public int getColumnCount() {
		return colCnt;
	}
	
	public void first() throws SQLException {
		if (getRow() <= 0) throw new SQLException("Resultset out of boundary.");
		rowIdx = 1;
		if (rs != null) rs.first();
	}

	public AnResultset beforeFirst() throws SQLException {
		if (getRow() > 0) rowIdx = 0;
		if (rs != null) rs.beforeFirst();
		return this;
	}
	
	/**idx start from 1. before(1) == beforeFirst().<br>
	 * As java.sql.Resultset don't support this method,
	 * calling this will throw an exception if this object are created from a java.sql.Resultset.
	 * @param idx
	 * @return this
	 * @throws SQLException
	 */
	public AnResultset before(int idx) throws SQLException {
		if (rs != null) throw new SQLException("before(int) can't been called when there is an associate java.sql.Resultset.");
		rowIdx = idx - 1;
		return this;
	}

	public void close() throws SQLException {
		if (rs != null) {
			rs.close();
			stmt.close();
			conn.close();
			rs = null;
		}
	}

	public boolean previous() throws SQLException {
		rowIdx--;
		if (rs != null) rs.previous();
		if (0 < rowIdx && rowIdx <= rowCnt) return true;
		else return false;
	}

	/**Get col name in raw case.<br>
	 * @param i start at 1
	 * @return column name or null
	 */
	public String getColumnName(int i) {
		for (String cn : colnames.keySet()) {
			if (((Integer)colnames.get(cn)[0]) == i)
				return (String) colnames.get(cn)[1];
		}
		return null;
	}


	/**Get col index
	 * @param colName
	 * @return col index
	 */
	public int getColumex(String colName) {
		return (int) colnames.get(colName.toUpperCase())[0];
	}

	/**Set column raw name.
	 * @param i index
	 * @param n name
	 */
	public void setColumnName(int i, String n) {
		for (String cn : colnames.keySet()) {
			if (((Integer)colnames.get(cn)[0]) == i) {
				colnames.get(cn)[1] = n;
				break;
			}
		}
	}
	
	/**/
	public int getRowCount() {
		return rowCnt;
	}

	public int getColCount() {
		return colCnt;
	}
	
	/**
	 * @param idx0 start at 0 
	 * @return row
	 * @throws SQLException
	 */
	public ArrayList<Object> getRowAt(int idx0) throws SQLException {
		if (results == null || idx0 < 0 || idx0 >= results.size()) 
			throw new SQLException("Row index out of boundary. idx: " + idx0);
		return results.get(idx0);
	}

	/**Set value to current row
	 * @param colIndex
	 * @param v
	 * @return this
	 * @throws SQLException 
	 */
	public AnResultset set(int colIndex, String v) throws SQLException {
		try {
			if (rowIdx <= 0 || results == null || results.get(rowIdx - 1) == null) return this;
			//if (results.get(rowIdx - 1).get(colIndex - 1) == null) return false;
			if (results.get(rowIdx - 1).size() < colIndex) return this;
			else {
				results.get(rowIdx - 1).set(colIndex - 1, v);
				return this;
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	/**Set value to current row
	 * @param colName
	 * @param v
	 * @return this
	 * @throws SQLException 
	 */
	public AnResultset set (String colName, String v) throws SQLException {
		return set((Integer)colnames.get(colName.toUpperCase())[0], v);
	}

	/**find the first row that contain a matched value in field <i>col</i>. Matching are done by <i>regex</i>.
	 * @param col
	 * @param regex
	 * @return row index or 0
	 * @throws SQLException
	 */
	public int findFirst(String col, String regex) throws SQLException {
		beforeFirst();
		Regex regx = new Regex(regex);
		while(next()) {
			String target = getString(col);
			if (regx.match(target))
				return rowIdx;
		}
		return 0;
	}

	public ArrayList<Object> getRowCells() {
		return results.get(rowIdx - 1);
	}
	
	/**Print ResutSet in System.out or System.err.
	 * @param err weather output in "out" or "err" 
	 * @param max mas rows to print
	 * @param includeCols include column of names.
	 * @return size
	 */
	public int printSomeData(boolean err, int max, String... includeCols) {
		try {
			printHeaders();
			if (includeCols != null && includeCols.length > 0) {
				if (!"*".equals(includeCols[0])) {
					for (int ix = 0; ix < includeCols.length; ix++)
						if (err) System.err.print("\t" + includeCols[ix]);
						else System.out.print("\t" + includeCols[ix]);

					// line feed
					if (err) System.err.println("");
					else System.out.println("");

					beforeFirst();
					while (next() && getRow() <= max) {
						for (String incCol : includeCols) 
							printcell(err, incCol);
						// end line
						if (err) System.err.println("");
						else System.out.println("");
					}
				}
				else {
					beforeFirst();
					while (next() && getRow() <= max) {
						for (int c = 1; c <= getColCount(); c++) 
							printcell(err, c);
				
						// end line
						if (err) System.err.println("");
						else System.out.println("");
					}
				}
			}
		} catch (Exception e) {}
		return results == null ? 0 : results.size();
	}

	private void printcell(boolean err, String c) throws SQLException {
		if (err)
			System.err.print("\t" + getString(c));
		else
			System.out.print("\t" + getString(c));
	}

	private void printcell(boolean err, int c) throws SQLException {
		if (err)
			System.err.print(String.format("%s : %s  ", c, getString(c)));
		else
			System.out.print(String.format("%s : %s  ", c, getString(c)));
	}

	private void printHeaders() {
		for (int c = 0; c < colnames.size(); c++)
			System.out.print(String.format("%s : %s\t", c + 1, getColumnName(c + 1)));

		System.out.println(String.format("\nrow count: %d", results == null ? 0 : results.size()));
	}

	/**Collect fields value that can be used in "IN" condition, e.g. 'v1', 'v2', ...
	 * @param rs
	 * @param fields
	 * @return ['row0 field-val', 'row1 field-val', ...]
	 * @throws SQLException
	 */
	public static String collectFields(AnResultset rs, String... fields) throws SQLException {
		String s = "";
		if (rs != null) {
			rs.beforeFirst();
			while (rs.next()) {
				for (String f : fields) {
					if (s.length() > 0)
						s += ",";
					// 2018.10.07 see UpdateBatch#recordUpdate() condition handling section
					// s += String.format("'%s'", rs.getString(f));
					s += rs.getString(f);
				}
			}
			rs.beforeFirst();
		}
		return s;
	}

	public String getString(int rowix, String field) throws SQLException {
		if (results == null || results.size() < rowix)
			return null;
		int colix = (Integer) colnames.get(field.toUpperCase())[0];
		// return getString(colix);

		try {
			if (rowix <= 0 || results == null || results.get(rowix - 1) == null) return null;
			if (results.get(rowix - 1).get(colix - 1) == null) return null;
			else {
				Object v = results.get(rowix - 1).get(colix - 1);
				return stringFormats != null && stringFormats.containsKey(v.getClass()) ?
						String.format(stringFormats.get(v.getClass()), v) : v.toString();
			}
		} catch (Exception e) {
			throw new SQLException(e.getMessage() + " Empty Results?");
		}

	}

	public int total() {
		return total < getRowCount() ? getRowCount() : total;
	}

	public AnResultset total(int total) {
		this.total = total;
		return this;
	}

	/**Try in-place convert all values to integer elements
	 * - expensive, especially with many non-integers.
	 * @return list
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ArrayList<?> getRowsInt() {
		for (ArrayList row : results) {
			for (int i = 0, sz = getColumnCount(); i < sz; i++) {
				try {
					row.set(i, Integer.valueOf((String) row.get(i)));
				}
				catch (Exception e) {}
			};
		}
		return results;
	}

	/**Convert results to an 1D array with elements from <i>col<i>
	 * @param col column name
	 * @return list of string
	 * @throws SQLException 
	 */
	public List<String> toArr(String col) throws SQLException {
		List<String> res = new ArrayList<String>(results.size());
		beforeFirst();
		while(next()) {
			res.add(getString(col));
		}
		beforeFirst();
		return res;
	}
	
	/**
	 * Iterating through the results and convert to hash map, like this:
	 * <pre>
	 HashMap &lt;String, SynState&gt; res = st
		.select(met.tbl, "l")
		.rs(st.instancontxt(conn, usr))
		.rs(0)
		.&lt;UserType&gt;map((currow) -&gt; {
			// create instance according current row
			return new UserType(currow.getString("id"));
		}); 
	 * </pre>
	 * TODO: This is a temporary way. Which will be moved to
	 * {@link io.odysz.semantic.DA.AbsConnect#select(String, ObjCreator, int) select()}.
	 * 
	 * @since 1.4.12
	 * @param keyField value of the field name used for map's key
	 * @param objCreator object creator (mapper)
	 * @return
	 * @return the hash map
	 * @throws SQLException
	 */
	public <T extends Anson> HashMap<String, T> map(String keyField, ObjCreator<T> objCreator)
			throws SQLException {
		HashMap<String, T> map = new HashMap<String, T>(results.size());
		beforeFirst();
		while(next()) {
			map.put(getString(keyField), objCreator.create(this));
		}
		beforeFirst();
		return map;
	}

	/**
	 * Construct a hash set using all value of field f.
	 * @since 1.4.12
	 * @param f
	 * @return set
	 * @throws SQLException
	 */
	public HashSet<String> set(String f) throws SQLException {
		HashSet<String> s = new HashSet<String>(results.size());
		beforeFirst();
		while(next()) {
			s.add(getString(f));
		}
		beforeFirst();
		return s;
	}

	/**
	 * A mutation of {@link #next()}. If has a next row, move index and
	 * return this, otherwise null.
	 * <p>For convenience if only needs to check the first row.</p>
	 * E.g. to check the updating records' existence:
	 * <pre>return ((AnResultset) transbuilder
	 * .select(targetable)
	 * .col(Funcall.count(pk), "c")
	 * .where(updt.where())
	 * .rs(syb.instancontxt(stx.connId(), usr))
	 * .rs(0))
	 * .nxt()
	 * .getInt("c") &gt; 0;
	 * </pre>
	 * @since 1.4.25
	 * @return this or null
	 * @throws SQLException
	 */
	public AnResultset nxt() throws SQLException {
		if (next())
			return this;
		else
			return null;
	}
	
	public boolean hasnext() {
		return (rs != null || results != null) && rowIdx < rowCnt;
		
	}
}

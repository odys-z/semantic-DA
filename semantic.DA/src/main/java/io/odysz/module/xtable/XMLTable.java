package io.odysz.module.xtable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class XMLTable {
	private static final String TAG = "XMLTable";
	private ILogger logger;
	
	protected String _tableID;
	protected HashMap<String, Integer> columns;
	protected HashMap<String, Integer> pkCols;
	protected ArrayList<String[]> rows;
	protected String[] currentRec;

	/** Construct an empty table according to cols and pk.</br>
	 * Table construction not finished without calling endTablePush().
	 * @param tableID
	 * @param cols
	 * @param pk
	 * @throws Exception 
	 */
	public XMLTable(String tableID, String cols, String pk, ILogger log) throws SAXException {
		isBuilding = true;
		// logger = XMLDataFactory.getLogger();
		logger = log;
		// build columns
		if (cols == null || cols.trim().equals("")) {
			logger.e(TAG, "Can not construct table " + tableID + " with empty col specification");
			return;
		}
		if (tableID == null || "".equals(tableID.trim()))
			tableID = "XMLTable.Generated Default Table ID";
		else _tableID = tableID;
		
		String[] colNames = cols.split(",");
		columns = new LinkedHashMap<String, Integer>();
		for (int i = 0; i < colNames.length; i++) {
			columns.put(colNames[i].trim(), i);
		}
		
		if (pk != null && !"".equals(pk.trim())) {
			pkCols = new LinkedHashMap<String, Integer>();
			String[] pkNames = pk.split(",");
			for (int i = 0; i < pkNames.length; i++) {
				pkCols.put(pkNames[i].trim(), columns.get(pkNames[i].trim()));
			}
		}
		
		rows = new ArrayList<String[]>();
	}

	/**
	 * Constructing an empty table, table structure value copied from parameters.
	 * Table construction not finished without calling endTablePush().
	 * This is useful for reduced rows and cols table copying.
	 * @param tableID
	 * @param fromColumns
	 * @param fromPkCols
	 */
	public XMLTable(String tableID, HashMap<String, Integer> fromColumns,
			HashMap<String, Integer> fromPkCols) {
		isBuilding = true;
		_tableID = tableID;
		columns = new HashMap<String, Integer>();
		for (String cname : fromColumns.keySet()) {
			columns.put(new String(cname), fromColumns.get(cname));
		}
		pkCols = new HashMap<String, Integer>();
		for (String cname : fromPkCols.keySet()) {
			pkCols.put(new String(cname), fromPkCols.get(cname));
		}
		rows = new ArrayList<String[]>();
	}

	/**
	 * value copy and construct a new table, according to parameters.
	 * @param newTableID
	 * @param fromColumns
	 * @param fromPkCols
	 * @param fromList
	 */
	public XMLTable(String newTableID, HashMap<String, Integer> fromColumns,
			HashMap<String, Integer> fromPkCols, ArrayList<String[]> fromList) {
		_tableID = newTableID;
		columns = new HashMap<String, Integer>();
		for (String cname : fromColumns.keySet()) {
			columns.put(new String(cname), fromColumns.get(cname));
		}
		pkCols = new HashMap<String, Integer>();
		for (String cname : fromPkCols.keySet()) {
			pkCols.put(new String(cname), fromPkCols.get(cname));
		}
		rows = new ArrayList<String[]>();
		for (String[] row : fromList) {
			String[] newRow = new String[columns.size()];
			for (String k : columns.keySet()) {
				if (row[columns.get(k)] != null)
					newRow[columns.get(k)] = new String(row[columns.get(k)]);
			}
			rows.add(newRow);
		}
		isBuilding = false;
	}

	/**This can be used to clone from FLResultset.<br>
	 * <b>Becareful, the cols and rows indexes in result set start at 1, but in XMLTable that start at 0!</b><br>
	 * All columns and rows idxes are -1.
	 * @param fromColumns
	 * @param fromPkCols
	 * @param fromList
	 */
	public XMLTable(HashMap<String, Integer> fromColumns,
			HashMap<String, Integer> fromPkCols, ArrayList<ArrayList<Object>> fromList) {
		_tableID = "table1";

		columns = new HashMap<String, Integer>();
		for (String cname : fromColumns.keySet()) {
			columns.put(new String(cname), fromColumns.get(cname) - 1);
		}

		if (fromPkCols != null && fromPkCols.size() > 0) {
			pkCols = new HashMap<String, Integer>();
			for (String cname : fromPkCols.keySet())
				pkCols.put(new String(cname), columns.get(cname));
		}

		rows = new ArrayList<String[]>();
		for (ArrayList<Object> row : fromList) {
			String[] newRow = new String[columns.size()];
			for (int i = 0; i < columns.size(); i++) {
				if (row.get(i) != null)
					newRow[i] = row.get(i).toString();
			}
			rows.add(newRow);
		}

		isBuilding = false;
	}

	protected void appendFieldValue(String colName, String fieldValue) throws SAXParseException {
		validateTableStruct();
		if (colName == null || "".equals(colName.trim()) || !columns.containsKey(colName.trim())) {
			logger.w(TAG, "Can not resolve colName to append value. Column ignored: " + colName + ", tableID: " + _tableID);
			return;
		}
		appendFieldValue(columns.get(colName), fieldValue);
	}

	protected void appendFieldValue(int colIdx, String fieldValue) throws SAXParseException {
		if (colIdx < 0 || colIdx > columns.size()) throw new SAXParseException("Column index out of bundary", null);
		currentRec[colIdx] = fieldValue;
	}

	protected void startRecordPush() throws SAXParseException {
		validateTableStruct();
		currentRec = new String[columns.size()];
	}

	/**
	 * Push the temp record into table rows.
	 * For data validation, checkPK shall be true.
	 * But it's a serious performance defect. Set checkPK to false when deploying a release version. 
	 * @param checkPK
	 * @throws SAXParseException
	 */
	protected void endRecordPush(boolean checkPK) throws SAXParseException {
		if (currentRec == null) {
			logger.e(TAG, String.format("Find a null record, check record format in table %s is correct.", _tableID));
			return;
		}
		
		if (checkPK && pkCols != null && pkCols.size() > 0) {
			String[] pkVals = new String[pkCols.size()];
			logger.i(TAG, "Parsing record...");
			// check null pk
			int c = 0;
			for (String col : pkCols.keySet()) {
				if (pkCols.get(col) == null || currentRec[pkCols.get(col)] == null) {
					logger.e(TAG, "PK value can not be null. tableID: " + _tableID + ", col: " + col);
					throw new SAXParseException("PK value can not be null. tableID: " + _tableID + ", col: " + col, null);
				}
				else {
					pkVals[c] = currentRec[pkCols.get(col)];
					logger.i(TAG, "\tPK = " + pkVals[c]);
					c++;
				}
			}
			// check duplicated pk
			if (currentRec != null && getRecordByPK(pkVals) != null) {
				logger.e(TAG, "Record ignored for duplicated pk from table " + _tableID + ": ");
				for (int i = 0; i < pkVals.length; i++)
					logger.e("    ", pkVals[i]);
			}
		}
		rows.add(currentRec);
		currentRec = null;
	}

	/**
	 * Find record according to pk values.
	 * pkVals is exactly the same order specified in xmlfile/table pk attribute.
	 * <br/>IMPORTANT: There are no index or any other performance facilities, be careful!
	 * @param pkVals
	 * @return target record
	 * @throws SAXParseException 
	 */
	public String[] getRecordByPK(String[] pkVals) throws SAXParseException {
		if (rows == null || rows.size() <= 0) return null;
		if (pkCols == null || pkCols.size() <= 0)
			throw new SAXParseException("Table " + _tableID + " can not support getRecordByPK() without PK specification.", null);
		for (int i = 0; i < rows.size(); i++) {
			boolean found = false;
			int j = 0;
			for (String k : pkCols.keySet()) {
				if (rows.get(i)[pkCols.get(k)].equals(pkVals[j])) {
					j++;
					found = true;
				}
				else {
					found = false;
					break;
				}
			}
			if (found) {
				return rows.get(i);
			}
		}
		return null;
	}

	private boolean isBuilding = false;
	public void startTablePush() {
		isBuilding = true;
	}
	public XMLTable endTablePush() {
		isBuilding = false;
		try {
			beforeFirst();
		} catch (SAXException e) {
			logger.e(TAG, "Why reached here?");
			e.printStackTrace();
		}
		return this;
	}

	private void validateTableStruct() throws SAXParseException {
		if (columns == null) throw new SAXParseException("Table Structure invalidate.", null);
	}

	////////////////////////////////////////////////////////////////////
	int rowIdx = -1;

	public XMLTable beforeFirst() throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		rowIdx = -1;
		return this;
	}
	
	/** go last record - getXXX(c) got last row's field.
	 * @throws SAXException
	 */
	public void last() throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		goAt(getRowCount() - 1);
	}

	public boolean next() throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		if (rows == null || rows.size() <= 0) return false;
		if (rowIdx < rows.size() - 1) {
			rowIdx++;
			return true;
		}
		else return false;
	}
	
	public boolean goAt(int position) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		if (rows == null || rows.size() <= 0) return false;
		if (position < 0 || position >= rows.size())
			throw new SAXException("Target position out of boundary.", null);
		rowIdx = position;
		return true;
	}

	public void end() throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		rowIdx = rows.size() - 1;
	}
	
	public boolean previous() throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		if (rows == null || rows.size() <= 0) return false;
		if (rowIdx > 0 && rowIdx < rows.size()) {
			rowIdx--;
			return true;
		}
		else return false;
	}
	
	public String[] getRow() throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		return rows.get(rowIdx);
	}
	
	/**
	 * @param col start from 0.
	 * @return target field value
	 * @throws SAXException
	 */
	public String getString(int col) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		validateTableStruct();
		if (columns.size() < col  || col < 0) throw new SAXException("Column index outof bundary.", null);
		return rows.get(rowIdx)[col];
	}

	public String getString(String colName) throws SAXException {
		validateTableStruct();
		int index = columns.get(colName);
		String ccc = getString(index);
		return ccc;
	}

	public String getStringAt(int row, String colName) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		validateTableStruct();
		int col = columns.get(colName);
		if (columns.size() < col  || col < 0) throw new SAXException("Column index outof bundary.", null);
		return rows.get(row)[col];
	}

	public String getTableID() {
		return _tableID;
	}

	public int getRowCount() {
		if (rows == null) return 0;
		return rows.size();
	}
	
	public int getRowIdx() {return rowIdx;}
	
	/**
	 * @param kv - [key = colname, val = fieldVal]
	 * @return record list
	 * @throws SAXException
	 */
	public ArrayList<String[]> findRecords(HashMap<String, String> kv) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		ArrayList<String[]> retList = new ArrayList<String[]>();
		
		if (rows == null) return retList;
		for (int i = 0; i < rows.size(); i++) {
			boolean matched = false;
			for (String col : kv.keySet()) {
				int c = columns.get(col);
				if (rows.get(i)[c] == null || !rows.get(i)[c].equals(kv.get(col))) {
					matched = false;
					break;
				}
				else matched = true;
			}
			if (matched) retList.add(rows.get(i));
		}
		return retList;
	}

	/**
	 * @param pkVals - [key = colname, val = fieldVal]
	 * @return target table
	 * @throws SAXException
	 */
	public XMLTable findRecordsTable(HashMap<String, String> pkVals) throws SAXException {
		XMLTable retTable = new XMLTable(_tableID, columns, pkCols);
		retTable.copyRows(findRecords(pkVals));
		retTable.endTablePush();
		return retTable;
	}

	public String[] getRowAt(int position) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		return rows.get(position);
	}

	private void copyRows(ArrayList<String[]> fromRows) {
		for (String[] row : fromRows) {
			String[] newRow = new String[columns.size()];
			for (int i = 0; i < columns.size(); i++) {
				if (row[i] != null) {
					String v = new String(row[i]);
					newRow[i] = v;
				}
			}
			rows.add(newRow);
		}
	}

	public HashMap<String, Integer> getColumns() {
		return columns;
	}

	public HashMap<String, Integer> getPKs() {
		return pkCols;
	}
	
	/**Clone a column index for id reference.<br>
	 * Sometimes the E2Engine will refere to a fixed name field to retrieve data, e.g. "itemId".
	 * @param newName
	 * @param oldName
	 * @throws SAXException
	 */
	public void cloneCol(String newName, String oldName) throws SAXException {
		if (columns == null)
			throw new SAXException("There is no columns to rename.");
		if (columns.containsKey(newName))
			throw new SAXException("New columns name already eaxists.");
		if (!columns.containsKey(oldName))
			throw new SAXException(String.format("There is no column named %s to rename.", oldName));
		int colIdx = columns.get(oldName);
		columns.put(newName, colIdx);
		if (pkCols != null && pkCols.containsKey(oldName))
			pkCols.put(newName, colIdx);
	}

	public ArrayList<String[]> getRows() {
		return rows;
	}
	
	public String[] getRows(String colName) throws SAXException {
		String[] cells = new String[rows.size()];
		beforeFirst();
		int i = 0;
		while (next()) {
			cells[i] = getString(colName);
			i++;
		}
		return cells;
	}

	public int getInt(String colName, int defaultVal) {
		try { return getInt(colName);}
		catch (Exception ex) { return defaultVal;}
	}
	
	public int getInt(String colName) throws SAXException {
		validateTableStruct();
		int index=columns.get(colName);
		return getInt(index);
	}

	public int getInt(int col, int defaultVal) {
		try { return getInt(col);}
		catch (Exception ex) { return defaultVal;}
	}
	
	private int getInt(int col) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		validateTableStruct();
		if (columns.size() < col  || col < 0) throw new SAXException("Column index outof bundary.", null);
		String v = rows.get(rowIdx)[col];
		return Integer.valueOf(v);
	}


	public int getIntAt(int row, String colName, int defaultVal) {
		try { return getIntAt(row, columns.get(colName), defaultVal); }
		catch (Exception ex) {return defaultVal;}
	}

	public int getIntAt(int row, int col, int defaultVal) {
		try { 
			String v = rows.get(row)[col];
			return Integer.valueOf(v);
		} catch (Exception ex) {return defaultVal;}
	}

	public boolean getBool(String colName, boolean defaultVal) {
		try { return getBool(colName);}
		catch (Exception ex) { return defaultVal;}
	}

	private boolean getBool(String colName) throws SAXException {
		validateTableStruct();
		int index=columns.get(colName);
		return getBool(index);
	}

	private boolean getBool(int col) throws SAXException {
		if (isBuilding) throw new SAXException("Can not move cursor while building table.", null);
		validateTableStruct();
		if (columns.size() < col  || col < 0) throw new SAXException("Column index outof bundary.", null);
		String v = rows.get(rowIdx)[col];
		if (v == null) return false;
		v = v.trim().toLowerCase();
		if (v.length() == 0 || v.length() > 4) return false;
		if ("1".equals(v) || "true".equals(v) || "y".equals(v) || "t".equals(v) || "yes".equals(v) || "ok".equals(v))
			return true;
		return false;
	}
	
	public String[] getStrings(String colName) throws SAXException {
		validateTableStruct();
		int index=columns.get(colName);
		String fstr = getString(index);
		if (fstr == null) return null;
		else
			return fstr.split(",", -1);
	}
	
	public float getFloat(String colName, float defaultVal) {
		try {
			String v = getString(colName).trim();
			return Float.valueOf(v);
		}catch (Exception e) {
//			e.printStackTrace();
			return defaultVal;
		}
	}
	
	public float[] getFloats(String colName, float defaultVal) throws SAXException {
		validateTableStruct();
		int index=columns.get(colName);
		String fstr = getString(index);
		if (fstr == null) return null;
		else {
			String[] fs = fstr.split(",", -1);
			float[] vfs = new float[fs.length];
			for (int i = 0; i < fs.length; i++) {
				try { vfs[i] = Float.valueOf(fs[i].trim());
				} catch (Exception e) { vfs[i] = defaultVal; }
			}
			return vfs;
		}
	}
	
	public int[] getInts(String colName, int defaultVal) throws SAXException {
		validateTableStruct();
		int index=columns.get(colName);
		String istr = getString(index);
		if (istr == null) return null;
		else {
			String[] ss = istr.split(",", -1);
			int[] vis = new int[ss.length];
			for (int i = 0; i < ss.length; i++) {
				try { vis[i] = Integer.valueOf(ss[i].trim());
				} catch (Exception e) { vis[i] = defaultVal; }
			}
			return vis;
		}
	}

	private HashMap<String, String> _tableAttrs;
	public void setXmlAttrs(Attributes attributes) {
		_tableAttrs = new HashMap<String, String>();
		for (int i = 0; i < attributes.getLength(); i++) {
			String k = attributes.getQName(i);
			String v = attributes.getValue(i);
			_tableAttrs.put(k, v);
		}
	}
	
	public String getAttribute(String attr, String defaultVal) {
		if ("package".equals(attr)) return "enlearn";
		if (_tableAttrs == null) return defaultVal;
		else {
			String v = _tableAttrs.get(attr);
			if ("package".equals(attr) && "0".equals(v)) return "defaultVal";
			else return v;
		}
	}
	
	public void setField(String colName, String val) throws SAXException {
		validateTableStruct();
		int index=columns.get(colName);
		rows.get(rowIdx)[index] = val;
	}
	
	/**If pk is duplicating, merging record is ignored. */
	public static final int duplicateIgnor = 101;
	/**If pk is duplicating, this obj's record is replaced with merging. */
	public static final int duplicateReplace = 102;
	/**Merge withTable to this object.<br/>
	 * Table name and colomns order can be different, but both table must have exactly the same columns and pk fields.<br/>
	 * <b>Note:</b><br/>
	 * This method is not suitable for large record's number. There is a performance problem,
	 * especially when both table has many duplicated records. xTable is not designed for DB functioning,
	 * it's designed for retrieve data such as App configurations.
	 * @param withTable
	 * @param duplicateMode one of {@link XMLTable#duplicateIgnor} and {@link XMLTable#duplicateReplace}
	 * @throws SAXException
	 */
	public void mergeWith(XMLTable withTable, int duplicateMode) throws SAXException {
		if (withTable == null)
			return;
		if (isBuilding || withTable.isBuilding)
			throw new SAXException(String.format("Can't merge (table=%s) if one or both tables is being built.",
					getTableID()));
		if (columns == null || withTable.columns == null || columns.size() != withTable.columns.size())
			throw new SAXException(String.format("Can't merge (table=%s) if both tables have different column size",
					getTableID()));
		for (String c : columns.keySet()) {
			if (!withTable.columns.containsKey(c))
				throw new SAXException(String.format("The other table(withTable=%s) doesn't have a column %s.",
						getTableID(), c));
		}
		
		HashMap<String, String> kv = new HashMap<String, String>();
		withTable.beforeFirst();
		while (withTable.next()) {
			if (pkCols != null) {
				kv.clear();
				for (String pk : pkCols.keySet()) {
					kv.put(pk, withTable.getString(pk));
				}
				ArrayList<String[]> dupList = findRecords(kv);
				if (dupList != null && dupList.size() > 0) {
					switch (duplicateMode) {
					case duplicateIgnor:
						// 1 ignore duplicated row - no cloning
						break;
					case duplicateReplace:
						// 2 replace row with cloned
						remove(kv);
						cloneRow(withTable);
						break;
					default:
						// x no correct duplicating way
						throw new SAXException(String.format("The record from merging table (row number = %s) is duplicating with this table, but no duplicate operation specificed correctly. Check parameter duplicateMode's value.", withTable.getRowIdx()));
					}
				}
				else 
					// 3 clone a non duplicated row
					cloneRow(withTable);
				}
			// clone row without pk
			else cloneRow(withTable);
		}
	}
	
	private void cloneRow(XMLTable withTable) throws SAXException {
//		HashMap<String, String> newRow = new HashMap<String, String>();
//		for (String c : columns.keySet()) {
//			newRow.put(c, withTable.getString(c));
//		}
//		appendRow(newRow);
		String[] newRow = new String[columns.size()];
		for (String c : columns.keySet()) {
			newRow[columns.get(c)] = withTable.getString(c);
		}
		rows.add(newRow);
	}
	
	public void appendRow(HashMap<String, String> row) throws SAXException {
//		if (!isBuilding || columns == null || columns.size() < row.size())
//			throw new SAXException("Can't append row if table been built or columns' size less than row's fields.");
//		
//		if (row == null || row.keySet() == null || row.keySet().size() == 0)
//			return;
//		
//		for (String rk : row.keySet()) {
//			if (!columns.containsKey(rk))
//				throw new SAXException(String.format("Row with column %s can't append to table %s.", rk, _tableID));
//		}
//		
//		String[] newRow = new String[columns.size()];
//		for (String c : columns.keySet()) {
//			newRow[columns.get(c)] = row.get(c);
//		}
//		rows.add(newRow);
		if (rows == null) {
			rows = new ArrayList<String[]>();
			rowIdx = -1;
		}
		insertRowAt(rows.size(), row);
	}
	
	public void insertRowAt(int rowIdx, HashMap<String, String> row) throws SAXException {
		if (!isBuilding || columns == null || columns.size() < row.size())
			throw new SAXException("Can't insert row if table been built or columns' size less than row's fields.");
		
		if (row == null || row.keySet() == null || row.keySet().size() == 0)
			return;
		
		for (String rk : row.keySet()) {
			if (!columns.containsKey(rk))
				throw new SAXException(String.format("Row with column %s can't append to table %s.", rk, _tableID));
		}
	
		String[] newRow = new String[columns.size()];
		for (String c : columns.keySet()) {
			newRow[columns.get(c)] = row.get(c);
		}
		rows.add(rowIdx, newRow);
	}
	
	public void remove(HashMap<String, String> kv) throws SAXException {
		if (isBuilding) throw new SAXException("Can not remove while building table.", null);
		
		if (rows == null) return;
		for (int i = 0; i < rows.size(); i++) {
			boolean matched = false;
			for (String col : kv.keySet()) {
				int c = columns.get(col);
				if (rows.get(i)[c] == null || !rows.get(i)[c].equals(kv.get(col))) {
					matched = false;
					break;
				}
				else matched = true;
			}
			if (matched) {
				rows.remove(i);
				return;
			}
		}
	}
}

package com.ibm.dpft.engine.core.dbo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.exception.DPFTDataFormatException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTDbo {
	private DPFTDboSet thisDboSet = null;
	public HashMap<String, Object> rowData = null;
	private HashMap<String, Object> changed_values = new HashMap<String, Object>();
	// Virtual data = non-persistence data
	private HashMap<String, Object> vData = new HashMap<String, Object>();
	private boolean tobeSaved = false;
	private String dboname = null;
	private boolean tobeAdded = false;
	private boolean tobeDeleted = false;

	public DPFTDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {

		this.rowData = data;
		this.dboname = dboname;
		this.setThisDboSet(thisDboSet);
	}

	public String getString(String colname, boolean toUpperCase) {
		return ((String) getColumnValue(colname, toUpperCase) != null) ? (String) getColumnValue(colname, toUpperCase)
				: (String) getVirtualData(colname);
	}

	public String getString(String colname) {
		return getString(colname, true);
	}

	public Date getDate(String colname) throws DPFTRuntimeException {
		Object value = (getColumnValue(colname) != null) ? getColumnValue(colname) : getVirtualData(colname);
		if (value instanceof String) {
			String sv = (String) value;
			SimpleDateFormat sdf = new SimpleDateFormat(DPFTUtil.getSupportedFormat(sv));
			try {
				return sdf.parse(sv);
			} catch (ParseException e) {
				Object[] params = { sv };
				throw new DPFTDataFormatException("SYSTEM", "DPFT0024E", params, e);
			}
		}
		return (Date) value;
	}

	private Object getVirtualData(String colname) {
		String col = colname.toUpperCase();
		return vData.get(col);
	}

	public Object getColumnValue(String colname) {

		return getColumnValue(colname, true);
	}

	public Object getColumnValue(String colname, boolean toUpperCase) {
		String col = "";
		if (toUpperCase) {
			col = colname.toUpperCase();
		} else {
			col = colname;
		}
		return (((changed_values.containsKey(col))) ? changed_values.get(col) : rowData.get(col));
	}

	public void setValue(String colname, Object value) {

		String col = colname.toUpperCase();
		if (!tobeAdded) {
			// row data has value for the column
			if (rowData.get(col) instanceof Date) {
				// Date type
				if (!((Date) rowData.get(col)).equals(value)) {
					changed_values.put(col, value);
					tobeSaved = true;
				} else {
					if (changed_values.containsKey(col))
						changed_values.remove(col);
				}
			} else if (rowData.get(col) instanceof String) {
				// String type
				if (!((String) rowData.get(col)).equals(value)) {
					changed_values.put(col, value);
					tobeSaved = true;
				} else {
					if (changed_values.containsKey(col))
						changed_values.remove(col);
				}
			} else {
				if (!rowData.containsKey(col))
					setNewValue(col, value);

				// null
				if (rowData.get(col) == null && value != null) {
					changed_values.put(col, value);
					tobeSaved = true;
				} else if (rowData.get(col) != null && value == null) {
					changed_values.put(col, value);
					tobeSaved = true;
				}
			}
		} else {
			setNewValue(col, value);
		}
	}

	public void setValue(DPFTDbo dbo, String[] ignore_cols) {

		String[] columns = dbo.getColumns();
		for (String col : columns) {
			if (col.equalsIgnoreCase("pid"))
				continue;
			boolean isIgnore = false;
			for (String ig_col : ignore_cols) {
				if (col.equalsIgnoreCase(ig_col)) {
					isIgnore = true;
					break;
				}
			}
			if (isIgnore)
				continue;
			this.setValue(col, dbo.getColumnValue(col));
		}
	}

	public void setValue(DPFTDbo dbo) {

		String[] columns = dbo.getColumns();
		for (String col : columns) {
			if (col.equalsIgnoreCase("pid"))
				continue;
			this.setValue(col, dbo.getColumnValue(col));
		}
	}

	private void setNewValue(String colname, Object value) {

		rowData.put(colname, value);
		this.getThisDboSet().regInsertCol(colname);
	}

	public boolean tobeSaved() {
		if (changed_values.isEmpty())
			tobeSaved = false;
		return tobeSaved;
	}

	public String getDboName() {
		return dboname;
	}

	public String[] getColumns() {

		return rowData.keySet().toArray(new String[rowData.keySet().size()]);
	}

	public Integer getPrimaryKeyValue() {

		return Integer.valueOf((String) rowData.get("PID"));
	}

	public boolean tobeAdded() {
		return tobeAdded;
	}

	public void setTobeAdded(boolean tobeAdded) {
		this.tobeAdded = tobeAdded;
	}

	public void delete() {
		tobeDeleted = true;
	}

	public boolean tobeDeleted() {
		return tobeDeleted;
	}

	public boolean isNull(String col, boolean toUpperCase) {
		if (this.getColumnValue(col, toUpperCase) == null)
			return true;
		if (this.getColumnValue(col, toUpperCase) instanceof String) {
			return ((String) this.getColumnValue(col, toUpperCase)).isEmpty();
		}
		return false;
	}

	public boolean isNull(String col) {
		return isNull(col, true);
	}

	public void setVirtualData(String col, String value) {

		String colname = col.toUpperCase();
		vData.put(colname, value);
	}

	public DPFTDboSet getThisDboSet() {
		return thisDboSet;
	}

	public void setThisDboSet(DPFTDboSet thisDboSet) {
		this.thisDboSet = thisDboSet;
	}

	public DPFTDboSet getDboSet(String selectAttrs, String tbname, String whereclause) throws DPFTRuntimeException {
		return thisDboSet.getDBConnector().getDboSet(selectAttrs, tbname, whereclause, this);
	}

	public DPFTDboSet getDboSet(String tbname, String whereclause) throws DPFTRuntimeException {
		return thisDboSet.getDBConnector().getDboSet(tbname, whereclause, this);
	}

	public DPFTDboSet getDboSet(String tbname) throws DPFTRuntimeException {
		return thisDboSet.getDBConnector().getDboSet(tbname, "", this);
	}

}

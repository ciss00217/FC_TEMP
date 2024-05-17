package com.ibm.dpft.engine.core.dbo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.exception.DPFTDboException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTDatetimeComparator;
import com.ibm.dpft.engine.core.util.DPFTDboComparator;
import com.ibm.dpft.engine.core.util.DPFTNumberComparator;
import com.ibm.dpft.engine.core.util.DPFTStringComparator;

public class DPFTDboSet {
	public static final int TYPE_DATETIME = 1;
	public static final int TYPE_NUMBER = 2;
	public static final int TYPE_CHAR = 3;
	public static final int ORDER_DESC = 0;
	public static final int ORDER_ASC = 1;

	//private DPFTConnector connector = null;
	private String dboname = null;
	private String selectAttrs = null;

	public String getDboname() {
		return dboname;
	}

	private List<DPFTDbo> dboset = null;
	private List<DPFTDbo> _dboset = null;
	private HashMap<String, String> filter = new HashMap<String, String>();
	private List<String> insert_cols = null;
	private String whereclause = null;
	private boolean tobeLoaded = false;
	private DPFTDbo parent = null;
	private boolean tobeRefreshed = true;
	private DPFTConfig db_cfg = null;

	public DPFTDboSet(DPFTConfig db_cfg, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		this.db_cfg = db_cfg;
		this.dboname = tbname;
		if (whereclause == null)
			whereclause = "";
		this.setWhere(whereclause);
		if (selectAttrs == null)
			selectAttrs = "";
		this.setSelectAttrs(selectAttrs);

		initialize(selectAttrs, whereclause);

	}

	public DPFTDboSet(DPFTConfig db_cfg, List<HashMap<String, Object>> data) {
		dboset = convert2DboInstances(data);
		this.db_cfg = db_cfg;
	}

	String initattrs = null;
	String initwhereclause = null;
	void initialize(String attrs,String whereclause) throws DPFTRuntimeException {
		try {
//			connector.setQuery(attrs,dboname, whereclause);
			initattrs = attrs;
			initwhereclause = whereclause;
			tobeLoaded = true;
		} catch (Exception e) {
			throw new DPFTDboException("SYSTEM", "DPFT0025E", e);
		}

	}

	HashMap<Integer, Boolean> booleanMap = null;
	public void setBoolean(int i, boolean val) throws DPFTRuntimeException {
		if (booleanMap == null) {
			booleanMap = new HashMap<Integer, Boolean>();
		}
		booleanMap.put(i, val);
//		try {
//			connector.getCurrentStatement().setBoolean(i, val);
//		} catch (SQLException e) {
//			throw new DPFTDboException("SYSTEM", "DPFT0025E", e);
//		}
	}

	public void load() throws DPFTRuntimeException {
		if (!tobeLoaded)
			return;
		DPFTConnector connector = null;
		try {
			connector = DPFTConnectionFactory.initDPFTConnector(db_cfg, false);
			connector.setQuery(initattrs,dboname, initwhereclause);
			if (booleanMap != null) {
				Iterator<Integer> it = booleanMap.keySet().iterator();
				while (it.hasNext()) {
					Integer integer = (Integer) it.next();
					boolean val = booleanMap.get(integer);
					connector.getCurrentStatement().setBoolean(integer, val);
				}
			}
			dboset = convert2DboInstances(connector.retrieveDataFromCurrentStmt());
			tobeLoaded = false;
		} catch (Exception e) {
			throw new DPFTDboException("SYSTEM", "DPFT0026E", e);
		} finally {
			if (connector != null) {
				try {
					connector.close();
				} catch (Exception e2) {
				}
			}
		}
	}

	public void clear() {
		if (dboset != null)
			dboset.clear();
	}

	List<DPFTDbo> convert2DboInstances(List<HashMap<String, Object>> data) {

		List<DPFTDbo> ds = new ArrayList<DPFTDbo>();
		for (HashMap<String, Object> d : data) {
			DPFTDbo dbo = getDboInstance(this.dboname, d);
			ds.add(dbo);
		}
		return ds;
	}

	protected DPFTDbo getDboInstance(String name, HashMap<String, Object> d) {
		return new DPFTDbo(name, d, this);
	}

	protected DPFTDbo getNewDboInstance(String name, HashMap<String, Object> new_data) {

		DPFTDbo dbo = getDboInstance(name, new_data);
		dbo.setTobeAdded(true);
		if (dboset == null)
			dboset = new ArrayList<DPFTDbo>();
		dboset.add(dbo);
		return dbo;
	}

	public boolean isEmpty() throws DPFTRuntimeException {
		if (tobeLoaded)
			load();
		return (dboset == null || dboset.isEmpty()) ? true : false;
	}

	public int count() throws DPFTRuntimeException {
		if (tobeLoaded)
			load();
		if (dboset == null)
			return 0;
		return dboset.size();
	}

	public DPFTDbo getDbo(int i) throws DPFTRuntimeException {
		if (tobeLoaded)
			load();
		if (i < dboset.size())
			return dboset.get(i);
		else
			return null;
	}

	public void reset() throws DPFTRuntimeException {

		initialize(selectAttrs, whereclause);
	}

	public void reset(String where) throws DPFTRuntimeException {

		initialize(selectAttrs, where);
	}

	public String getWhere() {
		return whereclause;
	}

	public void setWhere(String whereclause) {
		this.whereclause = whereclause;
	}

	public String getSelectAttrs() {
		return selectAttrs;
	}

	public void setSelectAttrs(String selectAttrs) {
		this.selectAttrs = selectAttrs;
	}

	public void close() throws DPFTRuntimeException {

		try {
//			connector.close();
		} catch (Exception e) {
			throw new DPFTDboException("SYSTEM", "COMM0003E", e);
		}
		// connector = null;
	}

	public void save() throws DPFTRuntimeException {

		if (tobeSaved() || tobeAdded() || tobeDeleted()) {
			removeResidualsDbo();
			doSave();
			if (tobeRefreshed){
				refresh();
			}
			
		}

	}

	protected void removeResidualsDbo() {
		Iterator<DPFTDbo> ir = dboset.iterator();
		while (ir.hasNext()) {
			DPFTDbo dbo = ir.next();
			if (dbo.tobeAdded() && dbo.tobeDeleted()){
				ir.remove();
			}
			
		}
	}

	public void deleteAll() {
		for (DPFTDbo dbo : dboset) {
			dbo.delete();
		}
	}

	public boolean tobeAdded() throws DPFTRuntimeException {

		for (int i = 0; i < this.count(); i++) {
			if (this.getDbo(i).tobeAdded())
				return true;
		}
		return false;
	}

	public boolean tobeDeleted() throws DPFTRuntimeException {

		for (int i = 0; i < this.count(); i++) {
			if (this.getDbo(i).tobeDeleted())
				return true;
		}
		return false;
	}

	public void refresh() throws DPFTRuntimeException {

		initialize(null,getDbosWhereStringByPID(dboset));
		load();
		// DPFTLogger.info(this, "Refreshed Current DboSet from DB...");
	}

	private String getDbosWhereStringByPID(List<DPFTDbo> dbos) {
		if (dbos.size() > 50 || dbos.isEmpty())
			return whereclause;

		StringBuilder sb = new StringBuilder();
		StringBuilder sb1 = new StringBuilder();
		sb.append("pid in (");
		for (DPFTDbo dbo : dbos) {
			sb1.append("'").append(dbo.getPrimaryKeyValue()).append("',");
		}
		sb.append(sb1.substring(0, sb1.length() - 1)).append(")");
		return sb.toString();
	}
	
	private void doSaveAction(List<DPFTDbo> updateDbolist, List<DPFTDbo> newDbolist, List<DPFTDbo> delDbolist) throws DPFTRuntimeException {
		try {
			// Delete Records if any
			DPFTConnector connector = null;
			if (!delDbolist.isEmpty()) {
				try {
					connector = DPFTConnectionFactory.initDPFTConnector(db_cfg);
					connector.setDelete(delDbolist);
					connector.doDelete();
					connector.commit();
				} finally {
					if (connector != null) {
						try {
							connector.close();
						} catch (Exception e) {
						}
					}
					connector = null;
				}
			}

			// For Insert/Update case, do insert first (in order to get new pid)
			if (!newDbolist.isEmpty()) {
				try {
					connector = DPFTConnectionFactory.initDPFTConnector(db_cfg);
					connector.setInsert(newDbolist);
					connector.doInsert();
					connector.commit();
					if (tobeRefreshed) {
						reset();
						load();
					}
				} finally {
					if (connector != null) {
						try {
							connector.close();
						} catch (Exception e) {
						}
					}
					connector = null;
				}
			}

			// Do update
			if (!updateDbolist.isEmpty()) {
				try {
					connector = DPFTConnectionFactory.initDPFTConnector(db_cfg);
				connector.setUpdate(updateDbolist);
				connector.doUpdate();
				connector.commit();
				} finally {
					if (connector != null) {
						try {
							connector.close();
						} catch (Exception e) {
						}
					}
					connector = null;
				}
			}
		} catch (SQLException e) {
			throw new DPFTDboException("SYSTEM", "DPFT0027E", e);
		}
	}
	
//	private void doSave() throws DPFTRuntimeException {
//		List<DPFTDbo> updateDbolist = new ArrayList<DPFTDbo>();
//		List<DPFTDbo> newDbolist = new ArrayList<DPFTDbo>();
//		List<DPFTDbo> delDbolist = new ArrayList<DPFTDbo>();
//		int actionCount = this.count() / 10;
//		for (int i = 0; i < this.count(); i++) {
//			DPFTDbo dbo = this.getDbo(i);
//			if (dbo.tobeSaved())
//				updateDbolist.add(dbo);
//			if (dbo.tobeAdded())
//				newDbolist.add(dbo);
//			if (dbo.tobeDeleted())
//				delDbolist.add(dbo);
//			
//			if(i>10787) {
//				System.out.println("i>10000"+i);
//			}
//			
//			if(i>20000) {
//				System.out.print("i>20000");
//			}
//
//			if (i > 0 && actionCount > 100 && i % actionCount == 0) {
//				doSaveAction(updateDbolist, newDbolist, delDbolist);
//				updateDbolist = new ArrayList<DPFTDbo>();
//				newDbolist = new ArrayList<DPFTDbo>();
//				delDbolist = new ArrayList<DPFTDbo>();
//			}
//		}
//		doSaveAction(updateDbolist, newDbolist, delDbolist);
//	}
	
	private void doSave() throws DPFTRuntimeException {
		List<DPFTDbo> updateDbolist = new ArrayList<DPFTDbo>();
		List<DPFTDbo> newDbolist = new ArrayList<DPFTDbo>();
		List<DPFTDbo> delDbolist = new ArrayList<DPFTDbo>();
		
		int actionCount =  this.count() / 10;
		
		for (int i = 0; i <  this.count(); i++) {
			DPFTDbo dbo = this.getDbo(i);
			if (dbo.tobeSaved())
				updateDbolist.add(dbo);
			if (dbo.tobeAdded())
				newDbolist.add(dbo);
			if (dbo.tobeDeleted())
				delDbolist.add(dbo);
			
			if(i>10787) {
				int totalCount= this.count();

				System.out.println("actionCount:"+actionCount);

				System.out.println("i:"+i);
				System.out.println("totalCount:"+totalCount);

			}
			
			if(i>20000) {
				System.out.print("i>20000");
			}

			if (i > 0 && actionCount > 100 && i % actionCount == 0) {
				doSaveAction(updateDbolist, newDbolist, delDbolist);
				updateDbolist = new ArrayList<DPFTDbo>();
				newDbolist = new ArrayList<DPFTDbo>();
				delDbolist = new ArrayList<DPFTDbo>();
				
				int totalCount= this.count();

				System.out.println("actionCount:"+actionCount);

				System.out.println("i:"+i);
				System.out.println("totalCount:"+totalCount);
				
				
			}
		}
		doSaveAction(updateDbolist, newDbolist, delDbolist);
	}


	private void doSave1() throws DPFTRuntimeException {

		List<DPFTDbo> updateDbolist = new ArrayList<DPFTDbo>();
		List<DPFTDbo> newDbolist = new ArrayList<DPFTDbo>();
		List<DPFTDbo> delDbolist = new ArrayList<DPFTDbo>();
		for (int i = 0; i < this.count(); i++) {
			DPFTDbo dbo = this.getDbo(i);
			if (dbo.tobeSaved())
				updateDbolist.add(dbo);
			if (dbo.tobeAdded())
				newDbolist.add(dbo);
			if (dbo.tobeDeleted())
				delDbolist.add(dbo);
		}
		try {
			// Delete Records if any
			DPFTConnector connector = null;
			if (!delDbolist.isEmpty()) {
				try {
					connector = DPFTConnectionFactory.initDPFTConnector(db_cfg);
					connector.setDelete(delDbolist);
					connector.doDelete();
					connector.commit();
				} finally {
					if (connector != null) {
						try {
							connector.close();
						} catch (Exception e) {
						}
					}
					connector = null;
				}
			}

			// For Insert/Update case, do insert first (in order to get new pid)
			if (!newDbolist.isEmpty()) {
				try {
					connector = DPFTConnectionFactory.initDPFTConnector(db_cfg);
					connector.setInsert(newDbolist);
					connector.doInsert();
					connector.commit();
					if (tobeRefreshed) {
						reset();
						load();
					}
				} finally {
					if (connector != null) {
						try {
							connector.close();
						} catch (Exception e) {
						}
					}
					connector = null;
				}
			}

			// Do update
			if (!updateDbolist.isEmpty()) {
				try {
					connector = DPFTConnectionFactory.initDPFTConnector(db_cfg);
				connector.setUpdate(updateDbolist);
				connector.doUpdate();
				connector.commit();
				} finally {
					if (connector != null) {
						try {
							connector.close();
						} catch (Exception e) {
						}
					}
					connector = null;
				}
			}
		} catch (SQLException e) {
			throw new DPFTDboException("SYSTEM", "DPFT0027E", e);
		}
	}

	public boolean tobeSaved() throws DPFTRuntimeException {

		for (int i = 0; i < this.count(); i++) {
			if (this.getDbo(i).tobeSaved())
				return true;
		}
		return false;
	}

	public DPFTDbo add() throws DPFTRuntimeException {
		if (tobeLoaded)
			load();
		if (insert_cols == null)
			insert_cols = new ArrayList<String>();
		HashMap<String, Object> new_data = new HashMap<String, Object>();
		return getNewDboInstance(this.dboname, new_data);
	}

	public DPFTConnector getDBConnector() throws DPFTRuntimeException {
		return DPFTConnectionFactory.initDPFTConnector(db_cfg);
	}

	public void setParent(DPFTDbo parent_dbo) {
		parent = parent_dbo;
	}

	public DPFTDbo getParent() {
		return parent;
	}

	public void orderby(String col, int type) throws DPFTRuntimeException {
		orderby(col, type, ORDER_DESC);
	}

	public void orderby(String col, int type, int order) throws DPFTRuntimeException {
		if (tobeLoaded)
			load();

		if (type == TYPE_DATETIME)
			Collections.sort(dboset, new DPFTDatetimeComparator(col, order));
		if (type == TYPE_NUMBER)
			Collections.sort(dboset, new DPFTNumberComparator(col, order));
		if (type == TYPE_CHAR)
			Collections.sort(dboset, new DPFTStringComparator(col, order));
	}

	public void orderby(DPFTDboComparator comparator) throws DPFTRuntimeException {
		if (tobeLoaded)
			load();
		Collections.sort(dboset, comparator);
	}

	public void filter(String column, String value) {
		// Filter records where column value = value
		filter.put(column, value);
		_applyfilter();
	}

	public void unfilter(String column) {
		if (filter.containsKey(column))
			filter.remove(column);
		_applyfilter();
	}

	public void unfilter() {
		filter.clear();
		_applyfilter();
	}

	private void _applyfilter() {
		if (_dboset == null)
			_dboset = dboset;

		if (filter.isEmpty()) {
			dboset = _dboset;
			_dboset = null;
			return;
		}

		ArrayList<DPFTDbo> filtered_dboset = new ArrayList<DPFTDbo>();
		for (DPFTDbo dbo : _dboset) {
			int match_count = 0;
			for (String col : filter.keySet()) {
				if (dbo.getColumnValue(col) instanceof String) {
					if (dbo.getString(col).equals(filter.get(col)))
						match_count++;
				}
			}
			if (match_count == filter.size())
				filtered_dboset.add(dbo);
		}
		dboset = filtered_dboset;
	}

	public List<String> getInsertCols() {
		return insert_cols;
	}

	public void regInsertCol(String colname) {
		if (insert_cols == null)
			return;
		if (!insert_cols.contains(colname))
			insert_cols.add(colname);
	}

	public boolean isTobeRefresh() {
		return tobeRefreshed;
	}

	public void setRefresh(boolean tobeRefreshed) {
		this.tobeRefreshed = tobeRefreshed;
	}

	public HashMap<String, String> getKeyValueMap(String key_col, String value_col) throws DPFTRuntimeException {
		HashMap<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < count(); i++) {
			map.put(this.getDbo(i).getString(key_col), this.getDbo(i).getString(value_col));
		}
		return map;
	}
}

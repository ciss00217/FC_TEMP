package com.ibm.dpft.engine.core.connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTStatement {
	private StringBuilder stmt_builder = null;
	private String stmtype = null;
	DPFTConfig dbCfg = null;
//	private PreparedStatement pstmt = null;
	private HashMap<Integer, Object> pstmtMap = null;
	private String[] update_cols = null;
	private String[] insert_cols = null;
	private List<DPFTDbo> update_dbos = null;
	private List<DPFTDbo> insert_dbos = null;
	private List<DPFTDbo> delete_dbos = null;
	private final int MAX_BATCH_SIZE = 50000;

	public DPFTStatement(String stmtype, DPFTConfig dbCfg) {
		
		if(stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_QUERY))
			initQueryStatement();
		if(stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_UPDATE))
			initUpdateStatement();
		if(stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_INSERT))
			initInsertStatement();
		if(stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_DELETE))
			initDeleteStatement();
		if(stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_SQL))
			initSQLStatement();
		
		this.stmtype = stmtype;
		this.dbCfg  = dbCfg;
	}

	private void initSQLStatement() {
		stmt_builder = new StringBuilder();
	}

	private void initDeleteStatement() {
		
		stmt_builder = new StringBuilder();
		stmt_builder.append("delete from ");
	}

	private void initInsertStatement() {
		
		stmt_builder = new StringBuilder();
		stmt_builder.append("insert into ");
	}

	private void initUpdateStatement() {
		
		stmt_builder = new StringBuilder();
		stmt_builder.append("update ");
	}

	private void initQueryStatement() {
		
		stmt_builder = new StringBuilder();
		stmt_builder.append("select ");
	}

	boolean autoCommit = true;
	public void prepareStatement() throws SQLException {
		
		if(stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_UPDATE)
		|| stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_INSERT)
		|| stmtype.equalsIgnoreCase(GlobalConstants.DB_STMT_TYPE_DELETE)){
//			conn.setAutoCommit(false);
			autoCommit = false;
		}
		
		//TODO
		//System.out.println("Execute prepareStatement:" + stmt_builder.toString());
		//DPFTLogger.debug(this, "Execute prepareStatement:" + stmt_builder.toString());
		pstmtMap = new HashMap<Integer, Object>();
	}

	public DPFTStatement selectAll() {
		
		if(stmt_builder.indexOf("select") != 0)
			return this;
		stmt_builder.append("* ");
		return this;
	}
	
	public DPFTStatement selectByAttrs(String selectAttrs) {
		
		if(stmt_builder.indexOf("select") != 0)
			return this;
		stmt_builder.append(selectAttrs);
		return this;
	}

	public DPFTStatement from(String tbname) {
		
		stmt_builder.append(" from ").append(tbname).append(" ");
		return this;
	}

	public DPFTStatement where(String whereclause) {
		/*where statement : col1=? and col2=? and col3 like ?...*/
		
		if(whereclause.isEmpty())
			return this;
		
		stmt_builder.append(" where ").append(whereclause);
		return this;
	}
	
	public DPFTStatement update(List<DPFTDbo> updateDbolist) {
		
		DPFTDbo template  = updateDbolist.get(0);
		stmt_builder.append(template.getDboName()).append(" set ");
		stmt_builder.append(appendColumnValueString(template)).append(" ");
		update_dbos = updateDbolist;
		return this;
	}
	
	public DPFTStatement insert(List<DPFTDbo> newDbolist) {
		
		DPFTDbo template  = newDbolist.get(0);
		stmt_builder.append(template.getDboName()).append(" (");
		StringBuilder sb_cols = new StringBuilder();
		StringBuilder sb_vals = new StringBuilder();
		List<String> col_list = template.getThisDboSet().getInsertCols();
		insert_cols = col_list.toArray(new String[col_list.size()]);
		for(String col: insert_cols){
			sb_cols.append(buildStmtColname(col)).append(",");
			sb_vals.append("?").append(",");
		}
		stmt_builder.append(sb_cols.substring(0, sb_cols.length()-1)).append(") values (")
		.append(sb_vals.substring(0, sb_vals.length()-1)).append(")");
		insert_dbos = newDbolist;
		return this;
	}
	
	private String buildStmtColname(String col) {
		
		StringBuilder sb = new StringBuilder();
		if(col.contains(GlobalConstants.Hyphen))
			sb.append("\"").append(col.toUpperCase()).append("\"");
		else
			sb.append(col.toUpperCase());
		return sb.toString();
	}

	public DPFTStatement delete(List<DPFTDbo> delDbolist) {
		
		DPFTDbo template  = delDbolist.get(0);
		stmt_builder.append(template.getDboName()).append(" ");
		delete_dbos = delDbolist;
		return this;
	}

	private String appendColumnValueString(DPFTDbo dbo) {
		
		String[] cols = dbo.getColumns();
		update_cols = new String[cols.length-1];
		StringBuilder sb1 = new StringBuilder();
		int i = 0;
		for(String col: cols){
			if(col.equalsIgnoreCase("PID"))
				continue;
			sb1.append(buildStmtColname(col)).append("=?,");
			update_cols[i] = col;
			i++;
		}
		return sb1.toString().substring(0, sb1.toString().length() - 1);
	}

	public void setBoolean(int i, boolean val) throws SQLException {
		
		if(pstmtMap == null)
			return;
		pstmtMap.put(i, val);
	}
	
	public void setString(int i, String val) throws SQLException {
		
		if(pstmtMap == null)
			return;
		pstmtMap.put(i, val);
	}
	
	public void setDate(int i, Date date) throws SQLException {
		
		if(pstmtMap == null)
			return;
		pstmtMap.put(i, new java.sql.Date(date.getTime()));
	}
	
	public void setInteger(int i, int val) throws SQLException {
		
		if(pstmtMap == null)
			return;
		pstmtMap.put(i, val);
	}


	public List<HashMap<String, Object>> doQuery() throws SQLException , DPFTRuntimeException {
		
		if(pstmtMap == null)
			return null;
		DPFTLogger.debug(this, "Execute Query SQL :" + stmt_builder.toString());
		DPFTConnector conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<HashMap<String, Object>> dboset = new ArrayList<HashMap<String, Object>>();
		try {
			conn = DPFTConnectionFactory.initDPFTConnector(dbCfg, false);
			conn.setAutoCommit(autoCommit);
			pstmt  = conn.prepareStatement(stmt_builder.toString());
			if (pstmtMap != null) {
				Iterator<Integer> it = pstmtMap.keySet().iterator();
				while (it.hasNext()) {
					Integer integer = (Integer) it.next();
					Object obj = pstmtMap.get(integer);
					if (obj instanceof Boolean) {
						pstmt.setBoolean(integer, (Boolean)obj);
					} else if (obj instanceof String) {
						pstmt.setString(integer, (String)obj);
					} else if (obj instanceof java.sql.Date) {
						pstmt.setDate(integer, (java.sql.Date)obj);
					} else if (obj instanceof Integer) {
						pstmt.setInt(integer, (Integer)obj);
					} else {
						pstmt.setObject(integer, obj);
					}
				}
			}
			rs = pstmt.executeQuery();
			while(rs.next()){
//				DPFTLogger.debug(this, "Fetch next query record");
				HashMap<String, Object> data = new HashMap<String, Object>();
				int col_cnt = rs.getMetaData().getColumnCount();
				for(int i = 0; i < col_cnt; i++){
					int index = i+1;
					String colName = rs.getMetaData().getColumnName(index);
					int type = rs.getMetaData().getColumnType(index);
					if(type == Types.TIMESTAMP){
						if(rs.getTimestamp(index) == null)
							data.put(colName, null);
						else
							data.put(colName, new Date(rs.getTimestamp(index).getTime()));
					}else if(type == Types.DATE){
						if(rs.getDate(index) == null)
							data.put(colName, null);
						else
							data.put(colName, new Date(rs.getDate(index).getTime()));
					}else{
						data.put(colName, rs.getString(index));
					}
//					DPFTLogger.debug(this, "ColName :" + colName + " Col Value = " + rs.getString(index));
				}
				dboset.add(data);
			}
		} finally {
			if (!autoCommit) {
				conn.commit();
				conn.setAutoCommit(true);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
		return dboset;
	}
	
	public void doUpdate() throws SQLException, DPFTRuntimeException {
		
		if(pstmtMap == null || update_dbos.isEmpty())
			return;
		DPFTLogger.debug(this, "Execute Update SQL :" + stmt_builder.toString());
		DPFTConnector conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = DPFTConnectionFactory.initDPFTConnector(dbCfg, false);
			conn.setAutoCommit(autoCommit);
			pstmt  = conn.prepareStatement(stmt_builder.toString());
			if (pstmtMap != null) {
				Iterator<Integer> it = pstmtMap.keySet().iterator();
				while (it.hasNext()) {
					Integer integer = (Integer) it.next();
					Object obj = pstmtMap.get(integer);
					if (obj instanceof Boolean) {
						pstmt.setBoolean(integer, (Boolean)obj);
					} else if (obj instanceof String) {
						pstmt.setString(integer, (String)obj);
					} else if (obj instanceof java.sql.Date) {
						pstmt.setDate(integer, (java.sql.Date)obj);
					} else if (obj instanceof Integer) {
						pstmt.setInt(integer, (Integer)obj);
					} else {
						pstmt.setObject(integer, obj);
					}
				}
			}
			int count = 0;
			for(DPFTDbo dbo: update_dbos){
				prepareBatch(pstmt, dbo, update_cols, true);
				if(++count % MAX_BATCH_SIZE == 0){
					pstmt.executeBatch();
					DPFTLogger.debug(this, "Execute Batch Update, updated record count:" + count);
				}
			}
			pstmt.executeBatch();
			DPFTLogger.info(this, update_dbos.get(0).getDboName() + " successfully updated " + update_dbos.size() + " records...");
		} finally {
			if (!autoCommit) {
				conn.commit();
				conn.setAutoCommit(true);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	public void doInsert() throws SQLException, DPFTRuntimeException {
		if(pstmtMap == null || insert_dbos.isEmpty())
			return;
		DPFTLogger.info(this, "Execute Insert SQL :" + stmt_builder.toString());
		DPFTLogger.debug(this, "Execute Delete SQL :" + stmt_builder.toString());
		DPFTConnector conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = DPFTConnectionFactory.initDPFTConnector(dbCfg, false);
			conn.setAutoCommit(autoCommit);
			pstmt  = conn.prepareStatement(stmt_builder.toString());
			if (pstmtMap != null) {
				Iterator<Integer> it = pstmtMap.keySet().iterator();
				while (it.hasNext()) {
					Integer integer = (Integer) it.next();
					Object obj = pstmtMap.get(integer);
					if (obj instanceof Boolean) {
						pstmt.setBoolean(integer, (Boolean)obj);
					} else if (obj instanceof String) {
						pstmt.setString(integer, (String)obj);
					} else if (obj instanceof java.sql.Date) {
						pstmt.setDate(integer, (java.sql.Date)obj);
					} else if (obj instanceof Integer) {
						pstmt.setInt(integer, (Integer)obj);
					} else {
						pstmt.setObject(integer, obj);
					}
				}
			}
			int count = 0;
			for(DPFTDbo dbo: insert_dbos){
				prepareBatch(pstmt, dbo, insert_cols, false);
				if(++count % MAX_BATCH_SIZE == 0){
					pstmt.executeBatch();
					DPFTLogger.debug(this, "Execute Batch Insert, Inserted record count:" + count);
				}
			}
			pstmt.executeBatch();
			DPFTLogger.info(this, insert_dbos.get(0).getDboName() + " successfully inserted " + insert_dbos.size() + " records...");
		} finally {
			if (!autoCommit) {
				conn.commit();
				conn.setAutoCommit(true);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	public void doDelete() throws SQLException, DPFTRuntimeException {
		
		if(pstmtMap == null || delete_dbos.isEmpty())
			return;
		DPFTLogger.debug(this, "Execute Delete SQL :" + stmt_builder.toString());
		DPFTConnector conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = DPFTConnectionFactory.initDPFTConnector(dbCfg, false);
			conn.setAutoCommit(autoCommit);
			pstmt  = conn.prepareStatement(stmt_builder.toString());
			if (pstmtMap != null) {
				Iterator<Integer> it = pstmtMap.keySet().iterator();
				while (it.hasNext()) {
					Integer integer = (Integer) it.next();
					Object obj = pstmtMap.get(integer);
					if (obj instanceof Boolean) {
						pstmt.setBoolean(integer, (Boolean)obj);
					} else if (obj instanceof String) {
						pstmt.setString(integer, (String)obj);
					} else if (obj instanceof java.sql.Date) {
						pstmt.setDate(integer, (java.sql.Date)obj);
					} else if (obj instanceof Integer) {
						pstmt.setInt(integer, (Integer)obj);
					} else {
						pstmt.setObject(integer, obj);
					}
				}
			}
			int count = 0;
			for(DPFTDbo dbo: delete_dbos){
				prepareBatch(pstmt, dbo, null, true);
				if(++count % MAX_BATCH_SIZE == 0){
					pstmt.executeBatch();
					DPFTLogger.debug(this, "Execute Batch Delete, Deleted record count:" + count);
				}
			}
			pstmt.executeBatch();
			DPFTLogger.info(this, delete_dbos.get(0).getDboName() + " successfully deleted " + delete_dbos.size() + " records...");
		} finally {
			if (!autoCommit) {
				conn.commit();
				conn.setAutoCommit(true);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	public void doSQL() throws SQLException , DPFTRuntimeException {
		if(pstmtMap == null)
			return;
		DPFTLogger.debug(this, "Execute SQL :" + stmt_builder.toString());
		DPFTConnector conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = DPFTConnectionFactory.initDPFTConnector(dbCfg, false);
			conn.setAutoCommit(autoCommit);
			pstmt  = conn.prepareStatement(stmt_builder.toString());
			if (pstmtMap != null) {
				Iterator<Integer> it = pstmtMap.keySet().iterator();
				while (it.hasNext()) {
					Integer integer = (Integer) it.next();
					Object obj = pstmtMap.get(integer);
					if (obj instanceof Boolean) {
						pstmt.setBoolean(integer, (Boolean)obj);
					} else if (obj instanceof String) {
						pstmt.setString(integer, (String)obj);
					} else if (obj instanceof java.sql.Date) {
						pstmt.setDate(integer, (java.sql.Date)obj);
					} else if (obj instanceof Integer) {
						pstmt.setInt(integer, (Integer)obj);
					} else {
						pstmt.setObject(integer, obj);
					}
				}
			}
			pstmt.execute();
		} finally {
			if (!autoCommit) {
				conn.commit();
				conn.setAutoCommit(true);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (Exception e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
				}
			}
		}
	}

	private void prepareBatch(PreparedStatement pstmt, DPFTDbo dbo, String[] cols, boolean has_pid) throws SQLException, DPFTRuntimeException {
		int col_length = 0;
		if(cols != null){
			for(int i = 0; i < cols.length; i++){
				int index = i+1;
				if(dbo.getColumnValue(cols[i]) == null){
					setString(index, "");
					pstmt.setString(index, "");
				}
				Object obj = dbo.getColumnValue(cols[i]);
				if(dbo.getColumnValue(cols[i]) instanceof String){
					setString(index, dbo.getString(cols[i]));
					pstmt.setString(index, dbo.getString(cols[i]));
				}else if(dbo.getColumnValue(cols[i]) instanceof Date){
					setDate(index, dbo.getDate(cols[i]));
					pstmt.setDate(index, new java.sql.Date(dbo.getDate(cols[i]).getTime()));
				} else {
					setString(index, "");
					pstmt.setString(index, "");
				}
			}
			col_length = cols.length;
		}
		if(has_pid){
			/*set whereclause pid*/
			setInteger(col_length + 1, dbo.getPrimaryKeyValue());
			pstmt.setInt(col_length + 1, dbo.getPrimaryKeyValue());
		}
		pstmt.addBatch();
	}

	public void close() throws SQLException {
		
		if(pstmtMap == null)
			return;
//		if(!pstmt.isClosed())
//			pstmt.close();
		pstmtMap = null;
	}

	public String getStatementType() {
		return stmtype;
	}
	
	public void setSQL(String script) {
		stmt_builder.append(script);
	}
}

package com.ibm.dpft.engine.core.connection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTConnector {
	//private static DPFTConnector sys_conn = null;
	class Info {
		Connection conn;
		Exception e;
		long createTime;
		Info(Connection conn){
			this.conn = conn;
			e = new Exception();
			createTime = System.currentTimeMillis();
		}
		public boolean isClosed() {
			try {
				return conn.isClosed();
			} catch (Exception e) {
				return true;
			}
		}
	}
	private static ArrayList<Connection> conn_pool = new ArrayList<Connection>();
	private Connection conn = null;
	private DPFTConfig db_cfg = null;
	private DPFTStatement stmt = null;
	private static DPFTConfig sys_cfg = null;
	private Object sem = new Object();

	public DPFTConnector(Connection conn, DPFTConfig cfg) {

		this.conn = conn;
		db_cfg = cfg;

		add2ConnectionPool(conn);
		// logPoolSummary();
	}

	private void add2ConnectionPool(Connection connection) {
//		synchronized (conn_pool) {
//			conn_pool.add(connection);
//		}
	}

	private void logPoolSummary() {
		synchronized (conn_pool) {
			StringBuilder sb = new StringBuilder();
			sb.append("Connection POOL Summary:\n")
					.append("Total JDBC Connection Instance = " + conn_pool.size() + " \n")
					.append("Total Open Connection = " + getActiveConnectionCount() + " \n")
					.append("Total Closed Connection = " + getClosedConnectionCount() + " \n");
			DPFTLogger.debug(this, "\n" + sb.toString());
		}
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		conn.setAutoCommit(autoCommit);
	}
	
	private int getClosedConnectionCount() {
		int count = 0;
		for (Connection conn : conn_pool) {
			try {
				if (conn.isClosed())
					count++;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}

	private int getActiveConnectionCount() {
		int count = 0;
		for (Connection conn : conn_pool) {
			try {
				if (!conn.isClosed())
					count++;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}

	public DPFTDboSet getDboSet(String selectAttrs, String tbname, String whereclause, DPFTDbo parent_dbo)
			throws DPFTRuntimeException {
		DPFTDboSet dboset = getDboSetInstance(selectAttrs, tbname, whereclause);
		dboset.setParent(parent_dbo);
		return dboset;
	}

	public DPFTDboSet getDboSet(String tbname, String whereclause, DPFTDbo parent_dbo) throws DPFTRuntimeException {
		DPFTDboSet dboset = getDboSetInstance(null, tbname, whereclause);
		dboset.setParent(parent_dbo);
		return dboset;
	}

	public DPFTDboSet getDboSet(String tbname) throws DPFTRuntimeException {

		return getDboSetInstance(tbname);
	}

	public DPFTDboSet getDboSet(String selectAttrs, String tbname, String whereclause) throws DPFTRuntimeException {
		return getDboSetInstance(selectAttrs, tbname, whereclause);
	}

	public DPFTDboSet getDboSet(String tbname, String whereclause) throws DPFTRuntimeException {
		return getDboSetInstance(null, tbname, whereclause);
	}

	DPFTStatement generateSQLStmt(String type) {

		return new DPFTStatement(type, db_cfg);
	}

	public void setQuery(String tbname, String whereclause) throws SQLException {

		setQuery(null, tbname, whereclause);
	}

	public void setQuery(String attrs, String tbname, String whereclause) throws SQLException {

		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_QUERY);
		if (StringUtils.isEmpty(attrs)) {
			/* Select All Records */
			stmt.selectAll().from(tbname).where(whereclause);
		} else {
			stmt.selectByAttrs(attrs).from(tbname).where(whereclause);
		}
		stmt.prepareStatement();
	}

	public DPFTConfig getDBConfig() {
		return db_cfg;
	}

	protected PreparedStatement prepareStatement(String sql) throws SQLException {
		return conn.prepareStatement(sql);
	}
	
//	public Connection getDBConnectionInstance() {
//		return conn;
//	}

	public DPFTStatement getCurrentStatement() {

		return stmt;
	}

	public List<HashMap<String, Object>> retrieveDataFromCurrentStmt() throws SQLException , DPFTRuntimeException {

		if (stmt == null)
			return null;
		return stmt.doQuery();

	}

	private DPFTDboSet getDboSetInstance(String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		try {
			synchronized (sem) {
				DPFTConnector sys_conn = null;
				try {
					//sys_conn = DPFTConnectionFactory.initDPFTConnector(db_cfg, false);
					
					sys_conn = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig(), false);
					sys_conn.setQuery("DPFT_DBO_DEF", "active=? and tbname=?");
					sys_conn.getCurrentStatement().setBoolean(1, true);
					sys_conn.getCurrentStatement().setString(2, tbname);
					List<HashMap<String, Object>> infolist = sys_conn.retrieveDataFromCurrentStmt();
					for (HashMap<String, Object> info : infolist) {
						String classname = (String) info.get("CLASSNAME");
						DPFTLogger.info(this, "DboSetInstance class :" + classname);
						DPFTLogger.info(this, "selectAttrs:" + selectAttrs + " from " + tbname + " where " + whereclause);
						return (DPFTDboSet) Class.forName(classname)
								.getConstructor(DPFTConfig.class, String.class, String.class, String.class)
								.newInstance(db_cfg, selectAttrs, tbname,
										whereclause);
					}
					return new DPFTDboSet(db_cfg, selectAttrs, tbname,
							whereclause);
				} finally {
					if (sys_conn != null) {
						sys_conn.close();
					}
				}
			}
		} catch (Exception e) {
			throw new DPFTConnectionException("SYSTEM", "COMM0005E", e);
		}
	}

	private DPFTDboSet getDboSetInstance(String tbname) throws DPFTRuntimeException {

		return getDboSetInstance(null, tbname, "");
	}

	public void close() {

		if (stmt != null) {
			try {
				stmt.close();
			} catch (Exception e) {
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
			}
		}
		// logPoolSummary();
	}

	public void setUpdate(List<DPFTDbo> updateDbolist) throws SQLException {

		if (updateDbolist.isEmpty()) {
			DPFTLogger.debug(this, "No Records need update...");
			return;
		}

		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_UPDATE);
		stmt.update(updateDbolist).where("pid=?");
		stmt.prepareStatement();

	}

	public void setInsert(List<DPFTDbo> newDbolist) throws SQLException {

		if (newDbolist.isEmpty()) {
			DPFTLogger.debug(this, "No Records need insert...");
			return;
		}

		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_INSERT);
		stmt.insert(newDbolist);
		stmt.prepareStatement();

	}

	public void setDelete(List<DPFTDbo> delDbolist) throws SQLException {

		if (delDbolist.isEmpty()) {
			DPFTLogger.debug(this, "No Records need delete...");
			return;
		}

		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_DELETE);
		stmt.delete(delDbolist).where("pid=?");
		stmt.prepareStatement();

	}

	public void commit() throws SQLException {

		if(!conn.isClosed()) {
			conn.commit();
		}
	}

//	public static DPFTConnector getSystemDBConnectorInstance() {
//		return sys_conn;
//	}

	public static void setSystemDBConnectorInstance(DPFTConfig sys_cfg) {
		DPFTConnector.sys_cfg = sys_cfg;
	}

	public void doUpdate() throws SQLException, DPFTRuntimeException {

		if (stmt.getStatementType().equals(GlobalConstants.DB_STMT_TYPE_UPDATE))
			stmt.doUpdate();
	}

	public void doInsert() throws SQLException, DPFTRuntimeException {

		if (stmt.getStatementType().equals(GlobalConstants.DB_STMT_TYPE_INSERT)) {
			stmt.doInsert();
		}
	}

	public void doDelete() throws SQLException, DPFTRuntimeException {

		if (stmt.getStatementType().equals(GlobalConstants.DB_STMT_TYPE_DELETE)) {
			stmt.doDelete();
		}
	}

	public DPFTDboSet execSQL(String script) throws SQLException, DPFTRuntimeException  {
		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_SQL);
		stmt.setSQL(script);
		stmt.prepareStatement();
		return new DPFTDboSet(db_cfg, this.retrieveDataFromCurrentStmt());
	}
	
	public void executeUpdateDelete(String script) throws SQLException, DPFTRuntimeException  {
		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_SQL);
		stmt.setSQL(script);
		stmt.prepareStatement();
		stmt.doSQL();
	}

	public void execUIDSQL(String script) throws SQLException, DPFTRuntimeException  {
		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_SQL);
		stmt.setSQL(script);
		stmt.prepareStatement();
		stmt.doSQL();
	}

	public void truncate(String tablename) throws SQLException, DPFTRuntimeException  {
		stmt = this.generateSQLStmt(GlobalConstants.DB_STMT_TYPE_SQL);
		// stmt.setSQL("truncate table " + tablename);
		stmt.setSQL("call TRUNC_TAB_PROC('" + tablename + "')");
		stmt.prepareStatement();
		stmt.doSQL();
	}

	public static void clearClosedConnection() {
		synchronized (conn_pool) {
			Iterator<Connection> ir = conn_pool.iterator();
			while (ir.hasNext()) {
				Connection con = ir.next();
				try {
					if (con.isClosed())
						ir.remove();
				} catch (SQLException e) {
				}
			}
		}
	}

	public static void closeAllConnection() {
		synchronized (conn_pool) {
			Iterator<Connection> ir = conn_pool.iterator();
			while (ir.hasNext()) {
				Connection con = ir.next();
				try {
					if (!con.isClosed())
						con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

}

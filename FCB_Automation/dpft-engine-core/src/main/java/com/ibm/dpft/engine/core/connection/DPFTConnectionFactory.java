package com.ibm.dpft.engine.core.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTEncryptUtil;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DPFTConnectionFactory {

	static Map<String, HikariDataSource> allPools = Collections.synchronizedMap(new HashMap<String, HikariDataSource>());
	
	 private static Connection getDBConnectionInstance(DPFTConfig cfg) throws DPFTRuntimeException {
		  String key = cfg.getConnectionString() + cfg.getDBhost() + cfg.getDBport() + cfg.getDBServiceName() + cfg.getDBtype() + cfg.getDBUserName() + cfg.getSid();
		  try {
		   if (cfg.getDBtype().equals(GlobalConstants.DB_ORACLE)) {
		    Class.forName("oracle.jdbc.driver.OracleDriver");
		    //return getDBConnectionInstance1(cfg);
		   } else if (cfg.getDBtype().equals(GlobalConstants.DB_MSSQL)) {
		    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		    //return getDBConnectionInstance1(cfg);
		   }
		   HikariDataSource ds = allPools.get(key);
		   if (ds == null) {
		    String getDBPass = DPFTEncryptUtil.getDecryptor().decrypt(cfg.getDBPassword());
		    HikariConfig config = new HikariConfig();
		          config.setJdbcUrl(cfg.getConnectionString());
		          config.setUsername(cfg.getDBUserName());
		          config.setPassword(getDBPass);
		          config.addDataSourceProperty( "minimumIdle" , "1" );
		          config.addDataSourceProperty( "maximumPoolSize" , "1000" );
		          config.addDataSourceProperty( "connectionTimeout" , "30000" );
		          config.addDataSourceProperty( "idleTimeout" , "600000" );
		          config.addDataSourceProperty( "maxLifetime" , "1800000" );
		          config.addDataSourceProperty( "autoCommit" , "true" );
		          config.setRegisterMbeans(true);
		          ds = new HikariDataSource(config);
		          allPools.put(key, ds);
		   }
			try {
				Connection conn = ds.getConnection();
				try {
					conn.commit();
				} catch (Exception e) {
				}
				conn.setAutoCommit(true);
				return conn;
			} catch (SQLException e) {
				if (e.getMessage() != null && e.getMessage().contains("Interrupted")) {
					Thread.currentThread().interrupt();
				}
				return getDBConnectionInstance1(cfg);
			}
		  } catch (Exception e) {
		   // TODO Auto-generated catch block
		   DPFTLogger.error(DPFTConnectionFactory.class.getName(), "SQL Error when getting connection instance:",
		     e);
		   throw new RuntimeException(e);//new DPFTConnectionException("SYSTEM", "COMM0004E", e);
		  }
		 }

	private static Connection getDBConnectionInstance1(DPFTConfig cfg) throws DPFTRuntimeException {

		try {
			DPFTLogger.info(DPFTConnectionFactory.class.getName(), "cfg.getDBtype()="+cfg.getDBtype());
			if (cfg.getDBtype().equals(GlobalConstants.DB_ORACLE)) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			} else if (cfg.getDBtype().equals(GlobalConstants.DB_MSSQL)) {
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			}

		} catch (ClassNotFoundException e) {
			DPFTLogger.error(DPFTConnectionFactory.class.getName(), "Fatal Error", e);
			
			throw new DPFTConnectionException("SYSTEM", "COMM0002E", e);
		}

		Connection conn = null;
		Properties prop = new Properties();
		int retry_limit = Integer.valueOf(DPFTEngine.getSystemProperties(GlobalConstants.DPFT_SYS_PROP_CONN_RETRY));
		int retry_num = 0;
		while (conn == null) {
			try {
				// DPFTLogger.debug(DPFTConnectionFactory.class.getName(), "JDBC
				// Connection = " + cfg.getConnectionString());
				// DPFTLogger.debug(DPFTConnectionFactory.class.getName(), "DB
				// User = " + cfg.getDBUserName());
				// DPFTLogger.debug(DPFTConnectionFactory.class.getName(), "DB
				// Password = " + cfg.getDBPassword());
				// DPFTLogger.debug(DPFTConnectionFactory.class.getName(),
				// "Connect to DB as " + cfg.getDBUserName());
				prop.setProperty("user", cfg.getDBUserName());
				prop.setProperty("password", cfg.getDBPassword());
				// prop.setProperty("oracle.jdbc.ReadTimeout", "14400000");
				// prop.setProperty("oracle.net.CONNECT_TIMEOUT", "14400000");
				// jdbc:sqlserver://10.8.220.153:1433;databaseName=AUTOMATIONDB;user=autodbo;password=*****;
				// String connectionString = cfg.getConnectionString() + "user=" +
				// cfg.getDBUserName() + ";password="
				// + DPFTEncryptUtil.getDecryptor().decrypt(cfg.getDBPassword());
				String connectionString = cfg.getConnectionString();
				// conn = DriverManager.getConnection(connectionString);
				String getDBPass = DPFTEncryptUtil.getDecryptor().decrypt(cfg.getDBPassword());
				//System.out.println(getDBPass);
				conn = DriverManager.getConnection(connectionString, cfg.getDBUserName(), getDBPass);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				DPFTLogger.error(DPFTConnectionFactory.class.getName(), "SQL Error when getting connection instance:",
						e);
				if (retry_num <= retry_limit) {
					try {
						// Retry after 10 sec.
						retry_num++;
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} else {
					throw new DPFTConnectionException("SYSTEM", "COMM0004E", e);
				}
			}
		}
		return conn;
	}

	public static DPFTConnector initDPFTConnector(DPFTConfig cfg, boolean closed) throws DPFTRuntimeException {
		DPFTConnector connector = new DPFTConnector(getDBConnectionInstance(cfg), cfg);
		if (closed) {
			try {
				connector.close();
			} catch (Exception e) {
//				throw new DPFTConnectionException("SYSTEM", "COMM0003E", e);
			}
		}
		return connector;
	}

	public static DPFTConnector initDPFTConnector(DPFTConfig cfg) throws DPFTRuntimeException {
		return initDPFTConnector(cfg, true);
	}
}

package com.ibm.dpft.engine.core.config;

import com.ibm.dpft.engine.core.common.GlobalConstants;

public class DPFTConfig {
	private String dbtype;
	private String dbhost;
	private String dbport;
	private String sid;
	private String servicename;
	private String username;
	private String password;

	public String getConnectionString() {
		StringBuilder sb = new StringBuilder();
		if (dbtype.equals(GlobalConstants.DB_ORACLE)) {
			sb.append("jdbc:oracle:thin:@").append(dbhost).append(":").append(dbport)
					.append((sid != null && !sid.isEmpty()) ? ":" + sid : "/" + servicename);
		} else if (dbtype.equals(GlobalConstants.DB_MSSQL)) {
			//"jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;user=UserName;password=*****";
			sb.append("jdbc:sqlserver://").append(dbhost).append(":").append(dbport)
			.append(";databaseName=" + servicename).append(";");
		}
		return sb.toString();
	}

	public String getDBtype() {
		return dbtype;
	}

	public void setDBtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getDBhost() {
		return dbhost;
	}

	public void setDBhost(String dbhost) {
		this.dbhost = dbhost;
	}

	public String getDBport() {
		return dbport;
	}

	public void setDBport(String dbport) {
		this.dbport = dbport;
	}

	public String getSid() {
		return sid;
	}

	public void setSid(String sid) {
		this.sid = sid;
	}

	public String getDBServiceName() {
		return servicename;
	}

	public void setDBServiceName(String servicename) {
		this.servicename = servicename;
	}

	public String getDBUserName() {
		return username;
	}

	public void setDBUserName(String username) {
		this.username = username;
	}

	public String getDBPassword() {
		return password;
	}

	public void setDBPassword(String password) {
		this.password = password;
	}
}

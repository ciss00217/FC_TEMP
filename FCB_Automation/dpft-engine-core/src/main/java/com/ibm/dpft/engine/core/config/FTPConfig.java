package com.ibm.dpft.engine.core.config;

public class FTPConfig {
	private String host = null;
	private int port = 21;
	private String user = null;
	private String pwd = null;
	private String ftptype = null;
	private String zip_pwd = null;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return pwd;
	}

	public void setZipPassword(String zip_pwd) {
		this.zip_pwd = zip_pwd;
	}
	
	public String getZipPassword() {
		return zip_pwd;
	}

	public void setPassword(String pwd) {
		this.pwd = pwd;
	}
	

	public String getFtptype() {
		return ftptype;
	}

	public void setFtptype(String ftptype) {
		this.ftptype = ftptype;
	}

}

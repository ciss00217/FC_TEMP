package com.ibm.dpft.engine.core.util;

import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public abstract class DPFTFileFTPUtil {
	private String localDir = null;
	private String remoteDir = null;
	private FTPConfig config = null;

	public DPFTFileFTPUtil(String ldir, String rdir, FTPConfig cfg) {
		localDir = ldir;
		remoteDir = rdir;
		config = cfg;
	}

	public abstract void doFTP_Out(String[] c_out_list, String[] file_out_list) throws DPFTRuntimeException;

	public abstract int doFTP_Get(String[] c_in_list, String[] file_in_list) throws DPFTRuntimeException;

	public abstract void doFTP_Del(String[] clist, String[] flist) throws DPFTRuntimeException;
	
	public abstract void doFTP_Move(String[] clist, String remoteDir) throws DPFTRuntimeException;

	public String getLocalDir() {
		return localDir;
	}

	public void setLocalDir(String localDir) {
		this.localDir = localDir;
	}

	public String getRemoteDir() {
		if (remoteDir == null) {
			remoteDir = "";
		}
		return remoteDir;
	}

	public void setRemoteDir(String remoteDir) {
		this.remoteDir = remoteDir;
	}

	public FTPConfig getConfig() {
		return config;
	}

	public void setConfig(FTPConfig config) {
		this.config = config;
	}

}

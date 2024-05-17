package com.ibm.dpft.engine.core.action;

import java.io.File;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTEncryptUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionFTPFileWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		return "DPFT_RES_FTP_DIR_DEF";
	}

	@Override
	public String getTableWatchCriteria() {
		return "active=1 and insta=0";
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	public FTPConfig getFTPConfig(String chal_name) {
		FTPConfig config = new FTPConfig();
		config.setHost(DPFTEngine.getSystemProperties("ftp." + chal_name + ".host"));
		config.setPort(Integer.parseInt(DPFTEngine.getSystemProperties("ftp." + chal_name + ".port")));
		config.setUser(DPFTEngine.getSystemProperties("ftp." + chal_name + ".user"));
		String pass = DPFTEncryptUtil.getDecryptor()
				.decrypt((DPFTEngine.getSystemProperties("ftp." + chal_name + ".password")));
		config.setPassword(pass);
		
		return config;
	}

	public String getFTPType(String chal_name) {
		String ftptype = DPFTEngine.getSystemProperties("ftp." + chal_name + ".ftptype");
		if (!StringUtils.isEmpty(ftptype)) {
			switch (DPFTEngine.getSystemProperties("ftp." + chal_name + ".ftptype")) {
			case "FTP":
				return "com.ibm.dpft.engine.core.util.DPFTBasicFTPUtil";
			case "SFTP":
				return "com.ibm.dpft.engine.core.util.DPFTSFTPUtil";
			case "FTPS":
				return "com.ibm.dpft.engine.core.util.DPFTFTPSUtil";
			default:
				return "com.ibm.dpft.engine.core.util.DPFTBasicFTPUtil";
			}
		} else {
			return "";
		}
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		ResFileDirSettingDboSet fSet = (ResFileDirSettingDboSet) this.getDataSet();
		for (int i = 0; i < fSet.count(); i++) {
			DPFTFileFTPUtil ftputil = null;
			try {
				ResFileDirSettingDbo dbo = (ResFileDirSettingDbo) fSet.getDbo(i);
				ftputil = (DPFTFileFTPUtil) Class.forName(getFTPType(dbo.getString("chal_name")))
						.getConstructor(String.class, String.class, FTPConfig.class).newInstance(dbo.getString("ldir"),
								dbo.getString("dir"), getFTPConfig(dbo.getString("chal_name")));
				String[] flist = dbo.getRemoteFileName();
				String[] clist = dbo.getRemoteCtrlName();
				// cdutil.lock();
				int rtnCode = ftputil.doFTP_Get(clist, flist);
				if (rtnCode == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS) {
					ftputil.doFTP_Del(clist, flist);
					ftputil.doFTP_Move(flist, dbo.getString("dir") + File.separator + "archive");
					ftputil.doFTP_Move(clist, dbo.getString("dir") + File.separator + "archive");
				}
				// cdutil.unlock();
			} catch (Exception e) {

				DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
				ex.handleException();
			}
		}
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {

	}

}

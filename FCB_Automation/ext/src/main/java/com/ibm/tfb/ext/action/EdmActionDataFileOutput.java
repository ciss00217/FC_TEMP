package com.ibm.tfb.ext.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTcdFTPUtil;

public class EdmActionDataFileOutput extends DPFTActionDataFileOutput {
	@Override
	public String getChannelName() {
		
		return "EDM";
	}

	@Override
	public FTPConfig getFTPConfig() {
		
		FTPConfig config = new FTPConfig();
		config.setHost("10.211.55.6");
		config.setPort(21);
		config.setUser("Administrator");
		config.setPassword("p@ssw0rd");
		return config;
	}

	@Override
	public DPFTFileFTPUtil getFTPUtil() throws DPFTRuntimeException {
		return new DPFTcdFTPUtil(getOutFileLocalDir(), getOutFileRemoteDir(), meta.getCDProfile());
	}

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

}

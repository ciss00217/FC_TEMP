package com.ibm.dpft.engine.core.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;

public interface DPFTFileOutputInterface {
	DPFTFileFormatter getFileFormatter() throws DPFTRuntimeException;
	HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() throws DPFTRuntimeException;
	String getOutFileLocalDir() throws DPFTRuntimeException;
	String getOutFileRemoteDir() throws DPFTRuntimeException;
	String getFileEncoding() throws DPFTRuntimeException;
	FTPConfig getFTPConfig();
	DPFTFileFTPUtil getFTPUtil() throws DPFTRuntimeException;
}

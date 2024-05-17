package com.ibm.dpft.engine.core.action;

import java.io.File;

import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;



public class DPFTActionInstaFTPFileWatch extends DPFTActionFTPFileWatch {
	
	@Override
	public void postAction() throws DPFTRuntimeException {
		DPFTLogger.info(this, "### Start DPFTActionInstaFTPFileWatch.postAction   ###");

		super.postAction();
		
		DPFTLogger.info(this, "### End DPFTActionInstaFTPFileWatch.postAction   ###");

		
	}
	
	@Override
	public String getTableWatchCriteria() {
		return "active=1 and insta=1";
	}
}

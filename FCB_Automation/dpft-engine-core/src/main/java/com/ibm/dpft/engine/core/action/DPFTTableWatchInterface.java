package com.ibm.dpft.engine.core.action;

import java.text.ParseException;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public interface DPFTTableWatchInterface {
	DPFTConfig getDBConfig();
	String getTableName();
	String getSelectAttrs();
	String getTableWatchCriteria() throws DPFTRuntimeException;
	String getTriggerKeyCol();
	void   postAction() throws DPFTRuntimeException;
	
}

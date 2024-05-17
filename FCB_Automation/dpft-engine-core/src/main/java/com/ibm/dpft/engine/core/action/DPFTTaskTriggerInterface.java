package com.ibm.dpft.engine.core.action;


import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public interface DPFTTaskTriggerInterface {
	DPFTTriggerMapDefDboSet getTriggerMap() throws DPFTRuntimeException;

}

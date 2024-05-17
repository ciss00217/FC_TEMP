package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionIbndTrigger extends DPFTActionTaskTrigger {

	@Override
	public DPFTTriggerMapDefDboSet getTriggerMap() throws DPFTRuntimeException {
		/*Define Channel Name and Task Plan ID Mapping*/
		try {
			return DPFTUtil.getIbndTriggerMap();
		} catch (DPFTConnectionException e) {
			throw new DPFTActionException(this, "SYSTEM", "DPFT0002E", e);
		}
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		
	}
}

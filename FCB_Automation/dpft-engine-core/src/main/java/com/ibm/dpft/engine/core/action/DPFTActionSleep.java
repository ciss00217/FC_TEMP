package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTActionSleep extends DPFTAction {
	private long sleepTime = 0;

	public DPFTActionSleep(long sleepTime) {
		super();
		this.sleepTime = sleepTime;
	}

	@Override
	public void action() {
		this.setPostActionSleepTime(sleepTime);
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}

	@Override
	public void finish() {
		

	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		
		
	}

	@Override
	public void clean() throws DPFTRuntimeException {
		
		
	}

}

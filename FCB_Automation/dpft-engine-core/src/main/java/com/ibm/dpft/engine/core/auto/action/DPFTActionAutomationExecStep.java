package com.ibm.dpft.engine.core.auto.action;

import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.auto.taskplan.DPFTAutomationProcessExecutor;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTActionAutomationExecStep extends DPFTAction {
	private DPFTAutomationProcessExecutor executor = null;
	private final static long sleep_time = 10000;

	public DPFTActionAutomationExecStep(DPFTAutomationProcessExecutor tp) {
		super();
		executor  = tp;
		
		/*default set watch intervals to 10 seconds*/
		this.setPostActionSleepTime(sleep_time);
	}

	@Override
	public void action() throws DPFTRuntimeException {
		executor.executeCurrentStep();
	}
	
	@Override
	public void finish() {

	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		executor.readRC(GlobalConstants.DPFT_AUTOMATION_PS_RC_ERROR);
	}

	@Override
	public void clean() throws DPFTRuntimeException {
		
		
	}

	

}

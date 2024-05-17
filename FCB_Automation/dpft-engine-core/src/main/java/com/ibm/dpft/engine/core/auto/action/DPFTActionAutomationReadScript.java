package com.ibm.dpft.engine.core.auto.action;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.action.DPFTActionReadScript;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationScriptReader;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTActionAutomationReadScript extends DPFTActionReadScript {
	private DPFTAutomationScriptReader reader = null;

	@Override
	public String getScriptName() {
		return DPFTEngine.getSystemProperties("sys.auto.script");
	}

	@Override
	public void loadScript() throws DPFTRuntimeException {
		reader = new DPFTAutomationScriptReader();
		reader.load(prop);
	}

	@Override
	public void finish() throws DPFTRuntimeException {
		((DPFTTaskRunner)DPFTTaskRunner.currentThread()).getMgr()
			.createNewRunnerByTaskPlanID(reader.getProcessManagerID()).start();
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		throw e;
	}

	@Override
	public void clean() throws DPFTRuntimeException {
		
	}

}

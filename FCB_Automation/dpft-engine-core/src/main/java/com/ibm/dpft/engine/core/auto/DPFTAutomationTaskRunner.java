package com.ibm.dpft.engine.core.auto;

import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.auto.taskplan.DPFTAutomationProcessExecutor;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTAutomationTaskRunner extends DPFTTaskRunner {

	public DPFTAutomationTaskRunner(DPFTBaseTaskPlan tp) {
		super(tp, ((DPFTTaskRunner) DPFTTaskRunner.currentThread()).getMgr());
	}

	/* Thread runs here */
	public void run() {
		try {
			this.getTaskPlan().doTask();
		} catch (InterruptedException e) {
			DPFTLogger.info(this, "Automation Task Running instance complete...");
		} catch (DPFTRuntimeException e) {
			DPFTLogger.error(this, "Automation Task Abnormally ternimated...");
		}

		try {
			this.getTaskPlan().finishUp();
		} catch (DPFTRuntimeException e) {
			DPFTLogger.error(this, "Error when Free Resource...", e);
		}
	}

	public void setGroupID(String gid) {

		((DPFTAutomationProcessExecutor) this.getTaskPlan()).setGroupID(gid);
	}

	public boolean isActive() {
		boolean isActive = getTaskPlan().getStatus().equals(GlobalConstants.DPFT_TP_STAT_COMP);
		DPFTLogger.info(this, "isActive:" + isActive);
		return !isActive;
	}

}

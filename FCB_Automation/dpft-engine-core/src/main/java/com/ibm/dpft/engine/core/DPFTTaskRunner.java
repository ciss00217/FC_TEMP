package com.ibm.dpft.engine.core;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTTaskRunner extends Thread {
	private DPFTBaseTaskPlan plan = null;
	private DPFTTaskRunnerManager mgr = null;

	public DPFTTaskRunner(DPFTBaseTaskPlan tp, DPFTTaskRunnerManager mgr){
		try {
			setTaskPlan(tp.getNewInstance());
		} catch (Exception e) {
			DPFTLogger.error(this, "Error when cloning task plan :", e);
			setTaskPlan(tp);
		} 
		this.mgr = mgr;
	}
	
	/*Thread runs here*/
	public void run () {
		try {
			plan.doTask();
		} catch (InterruptedException e) {
			DPFTLogger.error(this, "Task Interrupted...");
		} catch (DPFTRuntimeException e) {
			DPFTLogger.error(this, "Task Abnormally ternimated...");
		}
		try {
			plan.finishUp();
		} catch (DPFTRuntimeException e) {
			DPFTLogger.error(this, "Error when Free Resource...", e);
		}
	}

	public DPFTBaseTaskPlan getTaskPlan() {
		return plan;
	}

	public void setTaskPlan(DPFTBaseTaskPlan plan) {
		this.plan = plan;
	}

	public DPFTTaskRunnerManager getMgr() {
		return mgr;
	}

	public void setInitialData(DPFTDbo dbo) {
		
		plan.setInitialDataSet(dbo);
	}
}

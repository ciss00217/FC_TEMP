package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.auto.action.DPFTAutomationInstTableWatch;

public class DPFTWebServerCmdExecutor extends DPFTBaseTaskPlan {
	public DPFTWebServerCmdExecutor(String id) {
		super(id);
	}

	public DPFTWebServerCmdExecutor(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTAutomationInstTableWatch("WebServer"));
	}

	@Override
	public boolean isRecurring() {
		return true;
	}

}

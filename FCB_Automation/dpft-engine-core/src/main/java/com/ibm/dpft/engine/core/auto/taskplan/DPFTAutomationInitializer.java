package com.ibm.dpft.engine.core.auto.taskplan;

import com.ibm.dpft.engine.core.auto.action.DPFTActionAutomationReadScript;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;

public class DPFTAutomationInitializer extends DPFTBaseTaskPlan {
	public DPFTAutomationInitializer(String id) {
		super(id);
		
	}

	public DPFTAutomationInitializer(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionAutomationReadScript());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}

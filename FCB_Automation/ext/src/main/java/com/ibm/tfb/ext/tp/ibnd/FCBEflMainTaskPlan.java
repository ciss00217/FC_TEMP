package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.tfb.ext.action.FCBEflActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBEflActionPersonalDataTableWatch;

public class FCBEflMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBEflMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBEflMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		DPFTLogger.info(this, "Starting EFL...");
		this.getActionList().add(new FCBEflActionDataTableWatch());
		DPFTLogger.info(this, "Starting EFL...");
		this.getActionList().add(new FCBEflActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

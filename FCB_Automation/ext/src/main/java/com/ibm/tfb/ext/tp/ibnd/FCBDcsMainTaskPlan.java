package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBDcsActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBDcsActionPersonalDataTableWatch;

public class FCBDcsMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBDcsMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBDcsMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBDcsActionDataTableWatch());
		this.getActionList().add(new FCBDcsActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

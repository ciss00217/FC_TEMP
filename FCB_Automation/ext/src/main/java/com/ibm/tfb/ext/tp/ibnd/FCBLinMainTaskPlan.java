package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBLinActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBLinActionPersonalDataTableWatch;

public class FCBLinMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBLinMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBLinMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBLinActionDataTableWatch());
		this.getActionList().add(new FCBLinActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBCsmActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBCsmActionPersonalDataTableWatch;

public class FCBCsmMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBCsmMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBCsmMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBCsmActionDataTableWatch());
		this.getActionList().add(new FCBCsmActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

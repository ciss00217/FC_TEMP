package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBLeaActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBLeaActionPersonalDataTableWatch;

public class FCBLeaMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBLeaMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBLeaMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBLeaActionDataTableWatch());
		this.getActionList().add(new FCBLeaActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

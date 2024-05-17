package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBAppActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBAppActionPersonalDataTableWatch;

public class FCBAppMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBAppMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBAppMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBAppActionDataTableWatch());
		this.getActionList().add(new FCBAppActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}
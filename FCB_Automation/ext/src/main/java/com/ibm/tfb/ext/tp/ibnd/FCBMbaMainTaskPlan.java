package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBMbaActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBMbaActionPersonalDataTableWatch;

public class FCBMbaMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBMbaMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBMbaMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBMbaActionDataTableWatch());
		this.getActionList().add(new FCBMbaActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

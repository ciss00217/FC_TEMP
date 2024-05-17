package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBCtiActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBCtiActionPersonalDataTableWatch;

public class FCBCtiMainTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBCtiMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBCtiMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBCtiActionDataTableWatch());
		this.getActionList().add(new FCBCtiActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

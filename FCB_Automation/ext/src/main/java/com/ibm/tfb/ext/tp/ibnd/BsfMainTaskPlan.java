package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BsfActionDataTableWatch;

public class BsfMainTaskPlan extends DPFTBaseTaskPlan {

	public BsfMainTaskPlan(String id) {
		super(id);
	}	
	
	public BsfMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new BsfActionDataTableWatch());
//		this.getActionList().add(new BsfActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

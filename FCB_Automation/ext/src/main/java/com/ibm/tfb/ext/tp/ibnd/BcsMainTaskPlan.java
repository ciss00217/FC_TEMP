package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BcsActionDataTableWatch;
import com.ibm.tfb.ext.action.BcsActionPersonalDataTableWatch;

public class BcsMainTaskPlan extends DPFTBaseTaskPlan {

	public BcsMainTaskPlan(String id) {
		super(id);
	}

	public BcsMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new BcsActionDataTableWatch());
		this.getActionList().add(new BcsActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

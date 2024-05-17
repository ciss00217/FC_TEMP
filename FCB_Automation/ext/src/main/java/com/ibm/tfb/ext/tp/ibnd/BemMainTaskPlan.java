package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BemActionDataTableWatch;
import com.ibm.tfb.ext.action.BemActionPersonalDataTableWatch;

public class BemMainTaskPlan extends DPFTBaseTaskPlan {

	public BemMainTaskPlan(String id) {
		super(id);
	}
	
	public BemMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new BemActionDataTableWatch());
		this.getActionList().add(new BemActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.ExtActionDataTableWatch;

public class ExtMainTaskPlan extends DPFTBaseTaskPlan {

	public ExtMainTaskPlan(String id) {
		super(id);
	}

	public ExtMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		String chalCode = this.getId().substring(0, 3);
		this.getActionList().add(new ExtActionDataTableWatch(chalCode));
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

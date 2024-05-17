package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.SsmActionDataTableWatch;
import com.ibm.tfb.ext.action.SsmActionPersonalDataTableWatch;

public class SsmMainTaskPlan extends DPFTBaseTaskPlan {
	public SsmMainTaskPlan(String id) {
		super(id);
	}

	public SsmMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new SsmActionDataTableWatch());
		this.getActionList().add(new SsmActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

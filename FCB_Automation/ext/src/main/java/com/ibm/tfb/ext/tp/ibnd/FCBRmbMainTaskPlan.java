package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBRmbActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBRmbActionPersonalDataTableWatch;

public class FCBRmbMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBRmbMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBRmbMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBRmbActionDataTableWatch());
		this.getActionList().add(new FCBRmbActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}
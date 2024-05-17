package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBMvpActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBMvpActionPersonalDataTableWatch;

public class FCBMvpMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBMvpMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBMvpMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBMvpActionDataTableWatch());
		this.getActionList().add(new FCBMvpActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}
package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBOemActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBOemActionPersonalDataTableWatch;

public class FCBOemMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBOemMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBOemMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBOemActionDataTableWatch());
		this.getActionList().add(new FCBOemActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}
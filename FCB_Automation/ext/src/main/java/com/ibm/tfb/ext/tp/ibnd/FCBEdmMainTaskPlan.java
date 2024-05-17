package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BemActionDataTableWatch;
import com.ibm.tfb.ext.action.BemActionPersonalDataTableWatch;
import com.ibm.tfb.ext.action.FCBEdmActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBEdmActionPersonalDataTableWatch;

public class FCBEdmMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBEdmMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBEdmMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBEdmActionDataTableWatch());
		this.getActionList().add(new FCBEdmActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}
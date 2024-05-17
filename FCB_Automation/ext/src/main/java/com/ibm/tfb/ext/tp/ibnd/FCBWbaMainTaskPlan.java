package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBWbaActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBWbaActionPersonalDataTableWatch;

public class FCBWbaMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBWbaMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBWbaMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBWbaActionDataTableWatch());
		this.getActionList().add(new FCBWbaActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}
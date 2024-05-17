package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBsmsResponseDataTableWatch;

public class FCBSmsResponseTaskPlan extends DPFTBaseTaskPlan {

	public FCBSmsResponseTaskPlan(String id) {
		super(id);

	}

	public FCBSmsResponseTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBsmsResponseDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

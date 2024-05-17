package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBEdmActionDataFileOutput;

public class FCBEdmObndTaskPlan extends DPFTBaseTaskPlan {
	public FCBEdmObndTaskPlan(String id) {
		super(id);
	}

	public FCBEdmObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new FCBEdmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

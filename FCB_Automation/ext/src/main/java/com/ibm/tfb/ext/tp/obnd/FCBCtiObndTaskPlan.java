package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBCtiActionDataFileOutput;

public class FCBCtiObndTaskPlan extends DPFTBaseTaskPlan {
	public FCBCtiObndTaskPlan(String id) {
		super(id);
	}

	public FCBCtiObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new FCBCtiActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

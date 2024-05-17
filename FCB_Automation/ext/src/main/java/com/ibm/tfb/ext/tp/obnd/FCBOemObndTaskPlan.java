package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBOemActionDataFileOutput;

public class FCBOemObndTaskPlan extends DPFTBaseTaskPlan {
	public FCBOemObndTaskPlan(String id) {
		super(id);
	}

	public FCBOemObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new FCBOemActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BemActionDataFileOutput;

public class BemObndTaskPlan extends DPFTBaseTaskPlan {
	public BemObndTaskPlan(String id) {
		super(id);
	}

	public BemObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new BemActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

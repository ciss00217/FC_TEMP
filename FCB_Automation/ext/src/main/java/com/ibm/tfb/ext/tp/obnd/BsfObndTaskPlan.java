package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BsfActionDataFileOutput;

public class BsfObndTaskPlan extends DPFTBaseTaskPlan {
	public BsfObndTaskPlan(String id) {
		super(id);
	}

	public BsfObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new BsfActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BcsActionDataFileOutput;

public class BcsObndTaskPlan extends DPFTBaseTaskPlan {
	public BcsObndTaskPlan(String id) {
		super(id);
	}

	public BcsObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new BcsActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.SsmActionDataFileOutput;

public class LzsObndTaskPlan extends DPFTBaseTaskPlan {
	public LzsObndTaskPlan(String id) {
		super(id);
	}


	public LzsObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new SsmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

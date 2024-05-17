package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.InstaBsmActionDataFileOutput;

public class InstaBsmObndTaskPlan extends DPFTBaseTaskPlan {
	public InstaBsmObndTaskPlan(String id) {
		super(id);
	}

	public InstaBsmObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new InstaBsmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

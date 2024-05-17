package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.ExtActionDataFileOutput;

public class ExtObndTaskPlan extends DPFTBaseTaskPlan {
	public ExtObndTaskPlan(String id) {
		super(id);
	}

	public ExtObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		String chalCode = this.getId().substring(0, 3);
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new ExtActionDataFileOutput(chalCode));
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

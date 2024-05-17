package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBEflActionDataFileOutput;

public class FCBEflObndTaskPlan extends DPFTBaseTaskPlan {
	public FCBEflObndTaskPlan(String id) {
		super(id);
	}

	public FCBEflObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new FCBEflActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

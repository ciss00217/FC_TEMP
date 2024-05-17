package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BemActionDataFileOutput;
import com.ibm.tfb.ext.action.BfmActionDataFileOutput;

public class BfmObndTaskPlan extends DPFTBaseTaskPlan {
	public BfmObndTaskPlan(String id) {
		super(id);
	}

	public BfmObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new BfmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBWbaActionDataFileOutput;

public class FCBWbaObndTaskPlan extends DPFTBaseTaskPlan {
	
	public FCBWbaObndTaskPlan(String id) {
		super(id);
	}

	public FCBWbaObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new FCBWbaActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

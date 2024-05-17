package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.SemActionDataFileOutput;

public class SemObndTaskPlan extends DPFTBaseTaskPlan {
	public SemObndTaskPlan(String id) {
		super(id);
		
	}

	public SemObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new SemActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

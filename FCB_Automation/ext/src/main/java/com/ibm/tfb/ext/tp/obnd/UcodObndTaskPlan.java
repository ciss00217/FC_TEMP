package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.UcodActionDataFileOutput;
import com.ibm.tfb.ext.action.UcodActionDataTableWatch;

public class UcodObndTaskPlan extends DPFTBaseTaskPlan {
	public UcodObndTaskPlan(String id) {
		super(id);
		
	}

	public UcodObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new UcodActionDataTableWatch());
		this.getActionList().add(new UcodActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}

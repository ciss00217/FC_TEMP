package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBLinActionDataFileOutput;

public class FCBLinObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBLinObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBLinObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new FCBLinActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

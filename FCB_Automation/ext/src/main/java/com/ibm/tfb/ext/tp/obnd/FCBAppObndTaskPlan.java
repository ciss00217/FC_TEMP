package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBAppActionDataFileOutput;
import com.ibm.tfb.ext.action.FCBLinActionDataFileOutput;

public class FCBAppObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBAppObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBAppObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new FCBAppActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

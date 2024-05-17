package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBCsmActionDataFileOutput;
import com.ibm.tfb.ext.action.FCBCsmObndDataTableWatch;

public class FCBCsmObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBCsmObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBCsmObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new FCBCsmObndDataTableWatch());
		this.getActionList().add(new FCBCsmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

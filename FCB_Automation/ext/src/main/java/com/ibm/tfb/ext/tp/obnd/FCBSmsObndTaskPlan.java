package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBsmsObndDataTableWatch;

public class FCBSmsObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBSmsObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBSmsObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBsmsObndDataTableWatch());
		//this.getActionList().add(new FCBSmsActionApiCall());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

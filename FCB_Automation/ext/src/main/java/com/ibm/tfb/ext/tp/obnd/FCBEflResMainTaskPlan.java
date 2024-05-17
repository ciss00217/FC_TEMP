package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBEflActionResDataTableWatch;

public class FCBEflResMainTaskPlan extends DPFTBaseTaskPlan{
	public FCBEflResMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBEflResMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBEflActionResDataTableWatch());
	}
	
	@Override
	public boolean isRecurring() {
		return false;
	}

}

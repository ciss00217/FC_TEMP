package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBMvpActionResDataTableWatch;

public class FCBMvpResMainTaskPlan extends DPFTBaseTaskPlan{
	public FCBMvpResMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBMvpResMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBMvpActionResDataTableWatch());
	}
	
	@Override
	public boolean isRecurring() {
		return false;
	}

}

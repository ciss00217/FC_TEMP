package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBMbaActionDataFileOutput;

public class FCBMbaObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBMbaObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBMbaObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new FCBMbaActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

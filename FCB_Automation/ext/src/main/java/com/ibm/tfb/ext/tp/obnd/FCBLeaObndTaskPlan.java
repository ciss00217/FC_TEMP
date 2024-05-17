package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBLeaActionDataFileOutput;

public class FCBLeaObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBLeaObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBLeaObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new FCBLeaActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

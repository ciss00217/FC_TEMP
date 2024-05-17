package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBDcsActionDataFileOutput;

public class FCBDcsObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBDcsObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBDcsObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new FCBDcsActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

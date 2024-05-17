package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.IemActionDataFileOutput;

public class IemObndTaskPlan extends DPFTBaseTaskPlan {
	public IemObndTaskPlan(String id) {
		super(id);
		
	}

	public IemObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new IemActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

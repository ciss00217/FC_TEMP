package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.EdmActionDataFileOutput;

public class EdmObndTaskPlan extends DPFTBaseTaskPlan {
	public EdmObndTaskPlan(String id) {
		super(id);
		
	}

	public EdmObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
	    this.getActionList().add(new EdmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

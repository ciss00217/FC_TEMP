package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.WmsActionDataFileOutput;

public class WmsObndTaskPlan extends DPFTBaseTaskPlan {
	public WmsObndTaskPlan(String id) {
		super(id);
		
	}


	public WmsObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new WmsActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}
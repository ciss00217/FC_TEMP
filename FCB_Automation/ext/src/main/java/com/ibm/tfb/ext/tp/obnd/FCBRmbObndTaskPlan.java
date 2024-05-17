package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionObndDataTableWatch;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBMbaActionDataFileOutput;
import com.ibm.tfb.ext.action.FCBRmbActionDataFileOutput;

public class FCBRmbObndTaskPlan extends DPFTBaseTaskPlan {

	public FCBRmbObndTaskPlan(String id) {
		super(id);
		
	}
	
	public FCBRmbObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionObndDataTableWatch());
		this.getActionList().add(new FCBRmbActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}
}

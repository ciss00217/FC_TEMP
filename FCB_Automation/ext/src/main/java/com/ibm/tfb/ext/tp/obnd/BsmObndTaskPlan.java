package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BsmActionDataFileOutput;
import com.ibm.tfb.ext.action.BsmObndDataTableWatch;

public class BsmObndTaskPlan extends DPFTBaseTaskPlan {
	public BsmObndTaskPlan(String id) {
		super(id);
	}

	public BsmObndTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new BsmObndDataTableWatch());
	    this.getActionList().add(new BsmActionDataFileOutput());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}
}

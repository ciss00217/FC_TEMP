package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.BsmActionDataTableWatch;
import com.ibm.tfb.ext.action.BsmActionPersonalDataTableWatch;

public class BsmMainTaskPlan extends DPFTBaseTaskPlan {

	public BsmMainTaskPlan(String id) {
		super(id);
		
	}
	
	public BsmMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new BsmActionDataTableWatch());
		this.getActionList().add(new BsmActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}

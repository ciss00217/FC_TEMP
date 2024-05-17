package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.InstaBsmActionPersonalDataTableWatch;
import com.ibm.tfb.ext.action.InstaBsmActionDataTableWatch;

public class InstaBsmMainTaskPlan extends DPFTBaseTaskPlan {

	public InstaBsmMainTaskPlan(String id) {
		super(id);
		
	}
	
	public InstaBsmMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new InstaBsmActionDataTableWatch());
		this.getActionList().add(new InstaBsmActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}

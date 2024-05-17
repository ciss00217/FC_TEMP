package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.SemActionDataTableWatch;
import com.ibm.tfb.ext.action.SemActionPersonalDataTableWatch;

public class SemMainTaskPlan extends DPFTBaseTaskPlan {

	public SemMainTaskPlan(String id) {
		super(id);
		
	}
	
	public SemMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new SemActionDataTableWatch());
		this.getActionList().add(new SemActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}

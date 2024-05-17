package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.IemActionDataTableWatch;
import com.ibm.tfb.ext.action.IemActionPersonalDataTableWatch;

public class IemMainTaskPlan extends DPFTBaseTaskPlan {

	public IemMainTaskPlan(String id) {
		super(id);
		
	}
	
	public IemMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new IemActionDataTableWatch());
		this.getActionList().add(new IemActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		
		return false;
	}

}

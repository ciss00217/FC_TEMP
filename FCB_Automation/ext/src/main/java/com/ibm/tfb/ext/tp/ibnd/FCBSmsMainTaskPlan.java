
package com.ibm.tfb.ext.tp.ibnd;

import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.FCBSmsActionDataTableWatch;
import com.ibm.tfb.ext.action.FCBSmsActionPersonalDataTableWatch;

public class FCBSmsMainTaskPlan extends DPFTBaseTaskPlan {

	public FCBSmsMainTaskPlan(String id) {
		super(id);
	}
	
	public FCBSmsMainTaskPlan(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new FCBSmsActionDataTableWatch());
		this.getActionList().add(new FCBSmsActionPersonalDataTableWatch());
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

}

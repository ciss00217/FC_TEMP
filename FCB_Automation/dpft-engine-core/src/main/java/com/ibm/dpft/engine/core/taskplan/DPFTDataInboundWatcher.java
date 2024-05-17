package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionIbndTrigger;
import com.ibm.dpft.engine.core.action.DPFTActionIbndTableWatch;

public class DPFTDataInboundWatcher extends DPFTBaseTaskPlan {

	public DPFTDataInboundWatcher(String plan_id) {
		super(plan_id);
		
	}
	
	public DPFTDataInboundWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionIbndTableWatch());
		this.getActionList().add(new DPFTActionIbndTrigger());
		
	}

	@Override
	public boolean isRecurring() {
		
		return true;
	}

}

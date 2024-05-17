package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionObndTableWatch;
import com.ibm.dpft.engine.core.action.DPFTActionObndTrigger;

public class DPFTDataOutboundWatcher extends DPFTBaseTaskPlan {
	public DPFTDataOutboundWatcher(String id) {
		super(id);
		
	}


	public DPFTDataOutboundWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndTableWatch());
		this.getActionList().add(new DPFTActionObndTrigger());
	}

	@Override
	public boolean isRecurring() {
		
		return true;
	}

}

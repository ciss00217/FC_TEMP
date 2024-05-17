package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionInstaObndTableWatch;
import com.ibm.dpft.engine.core.action.DPFTActionObndTrigger;

public class DPFTInstaDataOutboundWatcher extends DPFTBaseTaskPlan {
	public DPFTInstaDataOutboundWatcher(String id) {
		super(id);
	}

	public DPFTInstaDataOutboundWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionInstaObndTableWatch());
		this.getActionList().add(new DPFTActionObndTrigger());
	}

	@Override
	public boolean isRecurring() {
		return true;
	}

}

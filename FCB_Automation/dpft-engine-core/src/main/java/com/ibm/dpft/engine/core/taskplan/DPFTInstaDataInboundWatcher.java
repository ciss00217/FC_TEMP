package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionIbndTrigger;
import com.ibm.dpft.engine.core.action.DPFTActionInstaIbndTableWatch;

public class DPFTInstaDataInboundWatcher extends DPFTBaseTaskPlan {
	public DPFTInstaDataInboundWatcher(String id) {
		super(id);
	}

	public DPFTInstaDataInboundWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionInstaIbndTableWatch());
		this.getActionList().add(new DPFTActionIbndTrigger());
	}

	@Override
	public boolean isRecurring() {
		return true;
	}

}

package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicTrigger;
import com.ibm.dpft.engine.core.action.DPFTActionObndTpWatch;
import com.ibm.dpft.engine.core.action.DPFTActionSleep;

public class DPFTDataOutboundPeriodicWatcher extends DPFTBaseTaskPlan {
	private final static long sleep_time = 600000;
	
	public DPFTDataOutboundPeriodicWatcher(String id) {
		super(id);
		
	}

	public DPFTDataOutboundPeriodicWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionObndTpWatch());
		this.getActionList().add(new DPFTActionObndPeriodicTrigger());
		this.getActionList().add(new DPFTActionSleep(sleep_time));
	}

	@Override
	public boolean isRecurring() {
		
		return true;
	}

}

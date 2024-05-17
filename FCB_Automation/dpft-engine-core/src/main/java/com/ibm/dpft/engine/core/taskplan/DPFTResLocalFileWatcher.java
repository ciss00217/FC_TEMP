package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionLocalFileWatch;
import com.ibm.dpft.engine.core.action.DPFTActionResControlTableWatch;
import com.ibm.dpft.engine.core.action.DPFTActionSleep;

public class DPFTResLocalFileWatcher extends DPFTBaseTaskPlan {
	private final static long sleep_time = 600000;
	
	public DPFTResLocalFileWatcher(String id) {
		super(id);
		
	}

	public DPFTResLocalFileWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
		
	}

	@Override
	public void setActionsForPlan() {
		
		this.getActionList().add(new DPFTActionLocalFileWatch());
		this.getActionList().add(new DPFTActionResControlTableWatch());
		this.getActionList().add(new DPFTActionSleep(sleep_time));
	}

	@Override
	public boolean isRecurring() {
		
		return true;
	}

}

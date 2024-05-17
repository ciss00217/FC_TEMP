package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionRemoteFileWatch;
import com.ibm.dpft.engine.core.action.DPFTActionSleep;

//com.ibm.dpft.engine.core.taskplan.DPFTResRemoteFileWatcher
public class DPFTResRemoteFileWatcher extends DPFTBaseTaskPlan {
	private final static long sleep_time = 1800000;

	public DPFTResRemoteFileWatcher(String id) {
		super(id);
	}

	public DPFTResRemoteFileWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionRemoteFileWatch());
		this.getActionList().add(new DPFTActionSleep(sleep_time));
	}

	@Override
	public boolean isRecurring() {
		return true;
	}

}

package com.ibm.dpft.engine.core.taskplan;

import com.ibm.dpft.engine.core.action.DPFTActionInstaFTPFileWatch;
import com.ibm.dpft.engine.core.action.DPFTActionInstaLocalFileWatch;
import com.ibm.dpft.engine.core.action.DPFTActionInstaRemoteFileWatch;
import com.ibm.dpft.engine.core.action.DPFTActionInstaResControlTableWatch;
import com.ibm.dpft.engine.core.action.DPFTActionSleep;

public class DPFTInstaFileWatcher extends DPFTBaseTaskPlan {
	private final static long sleep_time = 300000;
	
	public DPFTInstaFileWatcher(String id) {
		super(id);
	}

	public DPFTInstaFileWatcher(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
//		this.getActionList().add(new DPFTActionInstaRemoteFileWatch());
		
			this.getActionList().add(new DPFTActionInstaFTPFileWatch());
			this.getActionList().add(new DPFTActionInstaLocalFileWatch());
			this.getActionList().add(new DPFTActionInstaResControlTableWatch());
			this.getActionList().add(new DPFTActionSleep(sleep_time));
		}

	@Override
	public boolean isRecurring() {
		return true;
	}

}

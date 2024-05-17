package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public abstract class DPFTAction {
	private String status = null;
	private long post_action_sleep_time = 0;
	private DPFTAction previous_action = null;
	private DPFTDbo init_data = null;
	private DPFTDboSet resultSet = null;
	
	public DPFTAction(){
		status = GlobalConstants.DPFT_ACTION_STAT_INIT;
	}
	
	abstract public void action() throws DPFTRuntimeException;
	abstract public void finish() throws DPFTRuntimeException;
	abstract public void clean() throws DPFTRuntimeException;
	abstract public void handleException(DPFTActionException e) throws DPFTRuntimeException;
	
	public boolean isActionComplete(){
		return status.equals(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}
	
	public boolean isActionRunning(){
		return status.equals(GlobalConstants.DPFT_ACTION_STAT_RUN);
	}
	
	public void changeActionStatus(String stat){
		status = stat;
	}

	public long getPostActionSleepTime() {
		return post_action_sleep_time;
	}

	public void setPostActionSleepTime(long millis) {
		this.post_action_sleep_time = millis;
	}

	public boolean needSleep() {
		return false;
	}

	public DPFTAction getPreviousAction() {
		return previous_action;
	}

	public void setPreviousAction(DPFTAction previous_action) {
		this.previous_action = previous_action;
	}

	public void setInitialData(DPFTDbo init_data) {
		this.init_data = init_data;
	}
	
	public DPFTDbo getInitialData(){
		return this.init_data;
	}
	
	public DPFTDboSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(DPFTDboSet resultSet) {
		this.resultSet = resultSet;
	}
}

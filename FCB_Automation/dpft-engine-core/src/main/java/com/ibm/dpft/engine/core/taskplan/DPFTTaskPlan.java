package com.ibm.dpft.engine.core.taskplan;

public abstract class DPFTTaskPlan {
	private String id = null;
	
	public abstract void setActionsForPlan();
	public abstract boolean isRecurring();
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}

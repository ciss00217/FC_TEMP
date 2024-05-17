package com.ibm.tfb.ext.tp.obnd;

import com.ibm.dpft.engine.core.action.DPFTActionSleep;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.tfb.ext.action.TFBActionSendMail;

public class TFBMailSender extends DPFTBaseTaskPlan {
	private final static long sleep_time = 10000;
	
	public TFBMailSender(String tp) {
		super(tp);
	}

	public TFBMailSender(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new TFBActionSendMail());
		this.getActionList().add(new DPFTActionSleep(sleep_time));
	}

	@Override
	public boolean isRecurring() {
		return true;
	}

}

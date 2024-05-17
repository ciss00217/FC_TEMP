package com.ibm.tfb.ext.action;

import java.sql.SQLException;

import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.tfb.ext.util.EmailUtil;
import com.ibm.tfb.ext.util.HNEmailUtil;

public class HNActionSendMail extends DPFTAction {

	@Override
	public void action() throws DPFTRuntimeException {
		DPFTLogger.info(this, "HNActionSendMail action");
		HNEmailUtil util = new HNEmailUtil();
		try {
			util.send_alerts();
		} catch (SQLException e) {
			throw new DPFTActionException(this, "SYSTEM", "DPFT0045E", e);
		}
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}

	@Override
	public void finish() throws DPFTRuntimeException {
		
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}

	@Override
	public void clean() throws DPFTRuntimeException {
		
		
	}

	
}

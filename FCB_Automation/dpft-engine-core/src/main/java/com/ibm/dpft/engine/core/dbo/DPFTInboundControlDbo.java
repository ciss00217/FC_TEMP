package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;

public class DPFTInboundControlDbo extends DPFTDbo {

	public DPFTInboundControlDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);
	}

	public void complete() {
		this.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_COMP);
	}

	public void error() {
		this.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_ERROR);
	}

	public void run() {
		this.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_RUN);
	}

}

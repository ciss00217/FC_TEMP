package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;

public class DPFTActionInstaResControlTableWatch extends DPFTActionResControlTableWatch {
	@Override
	public String getTableWatchCriteria() {
		
		return "process_status='" + GlobalConstants.DPFT_CTRL_STAT_RUN + "' and insta=1";
	}
}

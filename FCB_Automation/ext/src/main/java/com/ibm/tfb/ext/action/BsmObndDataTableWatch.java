package com.ibm.tfb.ext.action;


import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicDataTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class BsmObndDataTableWatch extends DPFTActionObndPeriodicDataTableWatch {

	@Override
	public String getTableName() {

		return "O_BSM";
	}

	@Override
	public String getTableWatchCriteria() {
		String timeDate = DPFTUtil.getCurrentDateAsString();
		if (!DPFTEngine.getSystemProperties("BSM.rerun.date").isEmpty()) {
			timeDate = DPFTEngine.getSystemProperties("BSM.rerun.date");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("send_date = '").append(timeDate).append("' and process_status='")
				.append(GlobalConstants.O_DATA_OUTPUT).append("'");
		return sb.toString();
	}
}

package com.ibm.tfb.ext.action;

import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicDataTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDbo;

public class FCBCsmObndDataTableWatch extends DPFTActionObndPeriodicDataTableWatch {

	@Override
	public String getTableName() {

		return "O_CSM";
	}

	@Override
	public String getTableWatchCriteria() {
		DPFTTriggerMapDefDbo tmap = (DPFTTriggerMapDefDbo) this.getInitialData();
		String last_active_time = tmap.getString("last_active_time");
		StringBuilder sb = new StringBuilder();
		sb.append("process_time >= '").append(last_active_time).append("' and process_status='")
				.append(GlobalConstants.O_DATA_OUTPUT).append("'");
		return sb.toString();
	}
}

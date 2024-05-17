package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDbo;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public abstract class DPFTActionObndPeriodicDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableWatchCriteria() {
		DPFTTriggerMapDefDbo data = (DPFTTriggerMapDefDbo) this.getInitialData();
		return data.getDataSelectCriteria();
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		Object[] params = {getInitialData().getString("chal_name")};
		DPFTUtil.pushNotification(
				 new DPFTMessage("CUSTOM", "TFB00010I", params)
		);
		this.setResultSet(getDataSet());
	}
	
	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		DPFTTriggerMapDefDbo tmap = (DPFTTriggerMapDefDbo) this.getInitialData();
		DPFTTriggerMapDefDboSet tmapSet = tmap.getControlTableRecords();
		tmapSet.updateLastActiveTime();
		tmapSet.save();
		tmapSet.close();
		throw e;
	}
}

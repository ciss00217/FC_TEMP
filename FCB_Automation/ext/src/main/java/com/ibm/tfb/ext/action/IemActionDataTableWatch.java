package com.ibm.tfb.ext.action;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class IemActionDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		/* set Table watch db connection properties */
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {

		return "D_IEM";
	}

	@Override
	public String getTableWatchCriteria() {
		String where = DPFTUtil.getFKQueryString(getInitialData());
		return where;
	}

	@Override
	public String getTriggerKeyCol() {

		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		Object[] params = { "IEM EDM" };
		if (this.getDataSet().isEmpty())
			throw new DPFTActionException(this, "CUSTOM", "TFB00001E", params);
		// DPFTUtil.pushNotification(
		// DPFTUtil.getCampaignOwnerEmail(getInitialData().getString("camp_code")),
		// new DPFTMessage("CUSTOM", "TFB00008I", params)
		// );
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		// set correspond h_inbound record to error
		DPFTInboundControlDboSet hIbndSet = (DPFTInboundControlDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("H_INBOUND", DPFTUtil.getFKQueryString(getInitialData()));
		hIbndSet.error();
		hIbndSet.close();
		throw e;
	}

}

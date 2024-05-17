package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionObndDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		DPFTOutboundControlDbo data = (DPFTOutboundControlDbo) this.getInitialData();
		
		return data.getString("target_ds");
	}

	@Override
	public String getTableWatchCriteria() {
		DPFTOutboundControlDbo data = (DPFTOutboundControlDbo) this.getInitialData();
		return data.getDataSelectCriteria();
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		DPFTLogger.info(this, "DPFTActionObndDataTableWatch postAction~~~~~~~");
		Object[] params = {getTableName()};
		if(this.getDataSet().isEmpty())
			throw new DPFTActionException(this, "CUSTOM", "TFB00003E", params);
		DPFTOutboundControlDboSet hObndSet = (DPFTOutboundControlDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("H_OUTBOUND", getTableWatchCriteria() + " and process_status='" + GlobalConstants.DPFT_OBND_STAT_RUN + "'");
		if (hObndSet.getDbo(0).getString("QUANTITY").equals("0")) {
			throw new DPFTActionException(this, "CUSTOM", "TFB00003E", params);
		}
		Object[] params1 = {getInitialData().getString("chal_name")};
//		DPFTUtil.pushNotification(
//				 new DPFTMessage("CUSTOM", "TFB00010I", params1)
//		);
		DPFTLogger.info(this, "DPFTActionObndDataTableWatch count:"+getDataSet().count());
		this.setResultSet(getDataSet());
	}

	@Override
	public void finish() throws DPFTRuntimeException {
		
	}
	
	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		//set correspond h_inbound record to error
		DPFTOutboundControlDboSet hObndSet = (DPFTOutboundControlDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
											.getDboSet("H_OUTBOUND", getTableWatchCriteria() + " and process_status='" + GlobalConstants.DPFT_OBND_STAT_RUN + "'");
		hObndSet.error();
		hObndSet.close();
		throw e;
	}

	@Override
	public String getSelectAttrs() {
		// TODO Auto-generated method stub
		return null;
	}

}

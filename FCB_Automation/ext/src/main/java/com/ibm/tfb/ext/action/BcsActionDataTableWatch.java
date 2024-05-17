package com.ibm.tfb.ext.action;

import java.util.ArrayList;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;

public class BcsActionDataTableWatch extends DPFTActionTableWatch {
	private static String chalCode = "BCS";

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		return "D_" + chalCode;
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
		Object[] params = { chalCode };
		if (this.getDataSet().isEmpty())
			throw new DPFTActionException(this, "CUSTOM", "TFB00001E", params);
		// DPFTUtil.pushNotification(DPFTUtil.getCampaignOwnerEmail(getInitialData().getString("camp_code")),
		// new DPFTMessage("CUSTOM", "TFB00008I", params));

//		/* Get Data set from "D_CHANNEL" */
//		DPFTDboSet dSet = this.getDataSet();
//		/* Set Query criteria for "O_CHANNEL" */
//		String qString = DPFTUtil.getFKQueryString(dSet.getDbo(0));
//		if (qString == null) {
//			DPFTLogger.debug(this, "Built FK Query String Failed...");
//			return;
//		}
//
//		/* Get Data set from "O_CHANNEL" */
//		DPFTOutboundDboSet oSet = (DPFTOutboundDboSet) this.getDBConnector().getDboSet("O_" + chalCode, qString);
//		if (oSet.count() > 0) {
//			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
//			oSet.deleteAll();
//		}
//
//		/*
//		 * Validate records with personal info data & add record to outbound
//		 * data table
//		 */
//		ArrayList<String> cell_code_list = new ArrayList<String>();
//		ArrayList<String> cell_name_list = new ArrayList<String>();
//		for (int i = 0; i < dSet.count(); i++) {
//			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oSet.add();
//			new_dbo.setValue(dSet.getDbo(i));
//			new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
//			// find distinct cell code
//			if (!cell_code_list.contains(new_dbo.getString("cell_code"))) {
//				cell_code_list.add(new_dbo.getString("cell_code"));
//			}
//			if (!cell_name_list.contains(new_dbo.getString("cellname"))) {
//				cell_name_list.add(new_dbo.getString("cellname"));
//			}
//		}
//		oSet.setRefresh(false);
//		oSet.save();
//
//		/* Write usage codes to O_USAGECODE Table */
//		TFBUtil.processUsageCode(oSet, chalCode);
//
//		/* Write results to H_OUTBOUND Table */
//		TFBUtil.generateObndCtrlRecord(this.getDBConnector(), oSet, cell_code_list, cell_name_list, chalCode, true);
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		// set correspond h_inbound record to error
		DPFTInboundControlDboSet hIbndSet = (DPFTInboundControlDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("H_INBOUND", DPFTUtil.getFKQueryString(getInitialData()));
		hIbndSet.error();
		;
		hIbndSet.close();
		throw e;
	}

}

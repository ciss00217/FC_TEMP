package com.ibm.tfb.ext.action;

import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;

public class ExtActionDataTableWatch extends DPFTActionTableWatch {
	private String chalCode = "";
	private String contactID = "";
	private String sourceField = "";
	private String outputField = "";
	private String chalCom = "";

	public ExtActionDataTableWatch(String chalCode) {
		super();
		this.chalCode = chalCode;
		this.contactID = DPFTEngine.getSystemProperties(chalCode + ".contactID");
		this.sourceField = DPFTEngine.getSystemProperties(chalCode + ".sourceField");
		this.outputField = DPFTEngine.getSystemProperties(chalCode + ".outputField");
		this.chalCom = DPFTEngine.getSystemProperties(chalCode + ".com");
	}

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
		DPFTLogger.debug(this, "Built FK Query String..." + where);
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

		/* Get Data set from "D_CHANNEL" */
		DPFTDboSet dSet = this.getDataSet();
		// DPFTLogger.debug(this, "dSet camp_code:" +
		// dSet.getDbo(0).getColumnValue("camp_code"));
		/* Set Query criteria for "O_CHANNEL" */
		String qString = DPFTUtil.getFKQueryString(dSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}
		DPFTLogger.debug(this, "Built FK Query String = " + qString);

		/* Get Data set from "O_CHANNEL" */
		DPFTOutboundDboSet oSet = (DPFTOutboundDboSet) this.getDBConnector().getDboSet("O_" + chalCode, qString);
		if (oSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oSet.deleteAll();
		}
		DPFTConnector getContactInfoConnector = null;
		try {
			String contactSQL = "";
			if (!StringUtils.isEmpty(sourceField)) {
				DPFTConfig config = DPFTUtil.getDBConfig("MKTDM" + chalCom);
				getContactInfoConnector = DPFTConnectionFactory.initDPFTConnector(config, false);
				contactSQL = DPFTEngine.getSystemProperties(chalCode + ".getContactSQL");
			}
			/*
			 * Validate records with personal info data & add record to outbound
			 * data table
			 */
			ArrayList<String> cell_code_list = new ArrayList<String>();
			ArrayList<String> cell_name_list = new ArrayList<String>();
			for (int i = 0; i < dSet.count(); i++) {
				DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oSet.add();
				new_dbo.setValue(dSet.getDbo(i));
				String output = GlobalConstants.O_DATA_OUTPUT;
				if (!StringUtils.isEmpty(sourceField)) {
					String replaceId = dSet.getDbo(i).getString(contactID);
					String replacedContactSQL = "";
					try {
						replacedContactSQL = contactSQL.replace(contactID, replaceId);
						// DPFTLogger.debug(this, "Built get contact SQL String = "
						// + replacedContactSQL);
						DPFTDboSet iSet = getContactInfoConnector.execSQL(replacedContactSQL);
						if (iSet.getDbo(0) != null) {
							String contactField = iSet.getDbo(0).getString(this.sourceField);
							// DPFTLogger.debug(this, "GET contactField: " +
							// contactField);
							if (!StringUtils.isEmpty(contactField)) {
								new_dbo.setValue(this.outputField, contactField);
							} else {
								output = GlobalConstants.O_DATA_EXCLUDE;
							}
						} else {
							output = GlobalConstants.O_DATA_EXCLUDE;
						}

					} catch (SQLException e) {
						output = GlobalConstants.O_DATA_EXCLUDE;
						DPFTLogger.error(this, "Execute contactSQL Failed : " + replacedContactSQL);
						throw new DPFTAutomationException("SYSTEM", "AUTO0017E", e);
					}
				}

				new_dbo.setValue("process_status", output);
				// find distinct cell code
				if (!cell_code_list.contains(new_dbo.getString("cell_code"))) {
					cell_code_list.add(new_dbo.getString("cell_code"));
				}
				if (!cell_name_list.contains(new_dbo.getString("cellname"))) {
					cell_name_list.add(new_dbo.getString("cellname"));
				}
			}
			oSet.setRefresh(false);
			oSet.save();
			oSet.close();
			dSet.close();
			/* Write usage codes to O_USAGECODE Table */
			TFBUtil.processUsageCode(oSet, chalCode);

			/* Write results to H_OUTBOUND Table */
			TFBUtil.generateObndCtrlRecord(this.getDBConnector(), oSet, cell_code_list, cell_name_list, chalCode, true);
		} finally {
			if (getContactInfoConnector != null) {
				getContactInfoConnector.close();
			}
		}
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

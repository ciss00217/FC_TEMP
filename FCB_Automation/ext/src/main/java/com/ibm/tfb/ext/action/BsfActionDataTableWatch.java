package com.ibm.tfb.ext.action;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

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

public class BsfActionDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		/* set Table watch db connection properties */
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		return "D_BSF";
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
		Object[] params = { "SFA" };
		if (this.getDataSet().isEmpty())
			throw new DPFTActionException(this, "CUSTOM", "TFB00001E", params);
		// DPFTUtil.pushNotification(
		// DPFTUtil.getCampaignOwnerEmail(getInitialData().getString("camp_code")),
		// new DPFTMessage("CUSTOM", "TFB00008I", params)
		// );
		/* set db config = MKTDM */
		DPFTConfig config = DPFTUtil.getSystemDBConfig();

		/* Get Data set from "D_BSF" */
		DPFTDboSet dEdmSet = this.getDataSet();

		/* Get batch number */
		String timestamp = dEdmSet.getDbo(0).getString("TIMESTAMP").substring(0, 8);
		DPFTDboSet dEdmSetCount;
		String batchNum = "01";
		// DPFTLogger.info(this, "CAMPAIGN_CD:" +
		// dEdmSet.getDbo(0).getString("CAMPAIGN_CD") + " timestamp:"
		// + dEdmSet.getDbo(0).getString("TIMESTAMP"));
		DPFTConnector getCountConnector = null;
		try {
			getCountConnector = DPFTConnectionFactory.initDPFTConnector(config, false);
			dEdmSetCount = getCountConnector.execSQL(
					"SELECT ROW_NUMBER() OVER(ORDER BY TIMESTAMP ASC) AS ROWNUMBER,TIMESTAMP from D_BSF WHERE SUBSTRING (TIMESTAMP ,1 ,8 )  ='"
							+ timestamp + "' and CAMPAIGN_CD='" + dEdmSet.getDbo(0).getString("CAMPAIGN_CD")
							+ "' GROUP BY TIMESTAMP");
			if (dEdmSetCount.count() != 0 && dEdmSetCount.count() > 1) {
				for (int i = 0; i < dEdmSetCount.count(); i++) {
					// DPFTLogger.info(this, "check rownumber:" +
					// dEdmSetCount.getDbo(i).getColumnValue("ROWNUMBER")+"
					// timestamp:"+dEdmSetCount.getDbo(i).getColumnValue("TIMESTAMP"));
					if (dEdmSet.getDbo(0).getString("TIMESTAMP")
							.equals(dEdmSetCount.getDbo(i).getColumnValue("TIMESTAMP"))) {
						batchNum = String.format("%2s", dEdmSetCount.getDbo(i).getColumnValue("ROWNUMBER")).replace(" ",
								"0");
					}
				}
			}

		} catch (SQLException e) {
			throw new DPFTAutomationException("SYSTEM", "AUTO0017E", e);
		} finally {
			if (getCountConnector != null) {
				getCountConnector.close();
			}
		}

		/* Set Query criteria for "O_BSF" */
		String qString = DPFTUtil.getFKQueryString(dEdmSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_BSF" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oEdmSet = (DPFTOutboundDboSet) connector.getDboSet("O_BSF", qString);
		oEdmSet.load();
		/* Data set from "O_BSF" should be empty */
		if (oEdmSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oEdmSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound
		 * data table
		 */
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();

		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		for (int i = 0; i < dEdmSet.count(); i++) {
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oEdmSet.add();
			new_dbo.setValue(dEdmSet.getDbo(i));
			new_dbo.setValue("CUSTOMER_ID", dEdmSet.getDbo(i).getString("customer_id"));
			new_dbo.setValue("SFA_CPN_COMM_CD", "OUTBOUND");
			new_dbo.setValue("SFA_CPN_LOT", dEdmSet.getDbo(i).getString("TREATMENT_CODE"));
			new_dbo.setValue("SFA_CHANNEL", "BRANCH");
			new_dbo.setValue("CHANNEL_TEAM", "SFA");
			new_dbo.setValue("COMMUNICATION_CD", "CO_0001");
			new_dbo.setValue("BATCH_NUM", batchNum);
			String OFFR_EFFECTIVEDATE = dEdmSet.getDbo(i).getString("OFFR_EFFECTIVEDATE");
			String OFFR_EXPIRATIONDATE = dEdmSet.getDbo(i).getString("OFFR_EXPIRATIONDATE");
			try {
				new_dbo.setValue("COMMUNICATION_START_DATE", sdf.parse(OFFR_EFFECTIVEDATE));
				new_dbo.setValue("COMMUNICATION_END_DATE", sdf.parse(OFFR_EXPIRATIONDATE));
			} catch (ParseException e) {
				DPFTLogger.error(this, e.getMessage(), e);
				new_dbo.setValue("COMMUNICATION_START_DATE", "");
				new_dbo.setValue("COMMUNICATION_END_DATE", "");
			}
			new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
			String campaign_cd_trim = dEdmSet.getDbo(i).getString("CAMPAIGN_CD");
			if (!StringUtils.isEmpty(campaign_cd_trim)) {
				campaign_cd_trim = campaign_cd_trim.substring(0, campaign_cd_trim.length() - 9);
				new_dbo.setValue("CAMPAIGN_CD_TRIM", campaign_cd_trim);
			}
			new_dbo.setValue("CPN_LIST_FILE_NAME", "_SFA_" + campaign_cd_trim + "_" + batchNum + "_EX.txt");

			if (!cell_code_list.contains(new_dbo.getString("cell_code"))) {
				cell_code_list.add(new_dbo.getString("cell_code"));
			}
			if (!cell_name_list.contains(new_dbo.getString("cellname"))) {
				cell_name_list.add(new_dbo.getString("cellname"));
			}
			if ((i + 1) % 100 == 0)
				DPFTLogger.debug(this, "Processed " + (i + 1) + " records...");

		}

		long ps_fin_time = System.currentTimeMillis();
		DPFTLogger.info(this, "Processed total " + dEdmSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oEdmSet.setRefresh(false);
		oEdmSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oEdmSet, "BSF");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oEdmSet, cell_code_list, cell_name_list, "BSF", true);
		oEdmSet.close();
		dEdmSet.close();
		/* Set Result set for next action */
		setResultSet(oEdmSet);
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

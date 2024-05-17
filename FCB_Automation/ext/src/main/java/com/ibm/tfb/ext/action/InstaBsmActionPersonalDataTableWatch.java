package com.ibm.tfb.ext.action;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
import com.ibm.util.EncryptUtil;

public class InstaBsmActionPersonalDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		/* set Table watch db connection properties */
		return TFBUtil.getMKTDMConfig("MKTDMHNCB");
	}

	@Override
	public String getTableName() {
		return DPFTEngine.getSystemProperties("mktdb.tbl.cust.cont.HNCB");
	}

	@Override
	public String getTableWatchCriteria() throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		sb.append("party_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		return sb.toString();
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		/* set db config = MKTDM */
		DPFTConfig config = DPFTUtil.getSystemDBConfig();

		/* Get Data set from "D_BSM" */
		DPFTDboSet dMsgSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_BSM" */
		String qString = DPFTUtil.getFKQueryString(dMsgSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_BSM" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oMsgSet = (DPFTOutboundDboSet) connector.getDboSet("O_BIM", qString);
		oMsgSet.load();
		/* Data set from "O_BSM" should be empty */
		if (oMsgSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oMsgSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound
		 * data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		for (int i = 0; i < dMsgSet.count(); i++) {
			String cust_id = dMsgSet.getDbo(i).getString("customer_id");
			String mobile = custSet.getContactInfoByField("MOB_TEL_NUM", cust_id);

			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oMsgSet.add();
			new_dbo.setValue(dMsgSet.getDbo(i));
			new_dbo.setValue("CUSTOMERID", EncryptUtil.IdDecrypt(cust_id) + "|" + dMsgSet.getDbo(i).getString("TREATMENT_CODE"));
			DPFTDbo dMsg = dMsgSet.getDbo(i);

			if (dMsg.isNull("mobile_priority")) {
				/* use default mobile priority rule */
				mobile = custSet.getPrioritizedMobilePhone(dMsg.getString("customer_id"), "BSM",
						GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
			} else {
				/* use mobile_priority Setting */
				mobile = custSet.getPrioritizedMobilePhone(dMsg.getString("customer_id"), "BSM",
						dMsg.getString("mobile_priority"));
			}

			if (StringUtils.isEmpty(mobile)) {
				// person record doesn't have email info
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
			} else {
				// person record has email info
				new_dbo.setValue("dest_address", mobile);
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
			}
			// find distinct cell code
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
		DPFTLogger.info(this, "Processed total " + dMsgSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oMsgSet.setRefresh(false);
		oMsgSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oMsgSet, "BSM");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oMsgSet, cell_code_list, cell_name_list, "BIM", true);
		oMsgSet.close();
		dMsgSet.clear();
		/* Set Result set for next action */
		setResultSet(oMsgSet);
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		DPFTInboundControlDboSet hIbndSet = (DPFTInboundControlDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("H_INBOUND", DPFTUtil
						.getFKQueryString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet().getDbo(0)));
		hIbndSet.error();
		hIbndSet.close();
		throw e;
	}

}

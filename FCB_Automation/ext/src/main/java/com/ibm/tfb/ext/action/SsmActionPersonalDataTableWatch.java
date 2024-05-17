package com.ibm.tfb.ext.action;

import java.util.ArrayList;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBConstants;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class SsmActionPersonalDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		/* set Table watch db connection properties */
		return TFBUtil.getMKTDMConfig("MKTDMHNS");
	}

	@Override
	public String getTableName() {
		return DPFTEngine.getSystemProperties("mktdb.tbl.cust.cont.HNS");
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

		/* Get Data set from "D_SSM" */
		DPFTDboSet dSsmSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_SSM" */
		String qString = DPFTUtil.getFKQueryString(dSsmSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_SSM" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oSsmSet = (DPFTOutboundDboSet) connector.getDboSet("O_SSM", qString);
		oSsmSet.load();
		/* Data set from "O_SSM" should be empty */
		if (oSsmSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oSsmSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound
		 * data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		for (int i = 0; i < dSsmSet.count(); i++) {
			String cust_id = dSsmSet.getDbo(i).getString("customer_id");
			String mobile = "";

			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oSsmSet.add();
			new_dbo.setValue(dSsmSet.getDbo(i));
			String destname = dSsmSet.getDbo(i).getString("CAMP_CODE") + "$#"
					+ dSsmSet.getDbo(i).getString("TREATMENT_CODE") + "$#" + cust_id;
			new_dbo.setValue("DESTNAME", destname);

			if (new_dbo.isNull("mobile_priority")) {
				/* use default mobile priority rule */
				mobile = custSet.getPrioritizedMobilePhone(new_dbo.getString("customer_id"), "SSM", "01");
			} else {
				/* use mobile_priority Setting */
				mobile = custSet.getPrioritizedMobilePhone(new_dbo.getString("customer_id"), "SSM",
						new_dbo.getString("mobile_priority"));
			}

			if (mobile == null || ("").equals(mobile)) {
				// person record doesn't have email info
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
			} else {
				if (!mobile.startsWith("0") && !mobile.startsWith("+")) {
					mobile = "0" + mobile;
				}
				// person record has email info
				new_dbo.setValue("DSTADDR", mobile);
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
		DPFTLogger.info(this, "Processed total " + dSsmSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oSsmSet.setRefresh(false);
		oSsmSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oSsmSet, "SSM");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oSsmSet, cell_code_list, cell_name_list, "SSM", true);
		oSsmSet.close();
		dSsmSet.close();
		/* Set Result set for next action */
		setResultSet(oSsmSet);
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

package com.ibm.tfb.ext.action;

import java.util.ArrayList;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class BemActionPersonalDataTableWatch extends DPFTActionTableWatch {

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

		/* Get Data set from previous action */
		DPFTDboSet dEdmSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_Channel" */
		String qString = DPFTUtil.getFKQueryString(dEdmSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_Channel" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oEdmSet = (DPFTOutboundDboSet) connector.getDboSet("O_BEM", qString);
		oEdmSet.load();
		/* Data set from "O_Channel" should be empty */
		if (oEdmSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oEdmSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound
		 * data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		for (int i = 0; i < dEdmSet.count(); i++) {
			String cust_id = dEdmSet.getDbo(i).getString("customer_id");
			// String email = custSet.getContactInfoByField("EMAIL", cust_id);
			String email = "";
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oEdmSet.add();
			new_dbo.setValue(dEdmSet.getDbo(i));

			DPFTDbo dEdm = dEdmSet.getDbo(i);
			if (dEdm.isNull("email_priority")) {
				/* use default email priority rule */
				email = custSet.getPrioritizedEmail(dEdm.getString("customer_id"), "BEM",
						GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
			} else {
				/* use email_priority Setting */
				email = custSet.getPrioritizedEmail(dEdm.getString("customer_id"), "BEM",dEdm.getString("email_priority"));
			}
			if (email == null || ("").equals(email)) {
				// person record doesn't have email info
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
			} else if (email != null) { // edm判斷有無@並排除特殊符號
				if (email.contains("'") || email.contains("\"")) {
					email = email.replaceAll("['\"]", "");
				}
				if (email.contains("@") && !("").equals(email)) {
					new_dbo.setValue("email", email);
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
				} else {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
				}
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
		DPFTLogger.info(this, "Processed total " + dEdmSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oEdmSet.setRefresh(false);
		oEdmSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oEdmSet, "BEM");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oEdmSet, cell_code_list, cell_name_list, "BEM", true);
		oEdmSet.close();
		dEdmSet.close();
		/* Set Result set for next action */
		// setResultSet(oEdmSet);
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

	@Override
	public String getSelectAttrs() {
		return " CO_TYPE_CD, PARTY_ID, PROD_TYPE_CD, EMAIL ";
	}
}

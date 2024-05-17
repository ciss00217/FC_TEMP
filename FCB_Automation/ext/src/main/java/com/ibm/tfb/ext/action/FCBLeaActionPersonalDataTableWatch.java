package com.ibm.tfb.ext.action;

import java.util.ArrayList;
import java.util.HashMap;

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
import com.ibm.tfb.ext.dbo.MKTCustContAddOnline;
import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FCBLeaActionPersonalDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		/* set Table watch db connection properties */
		return TFBUtil.getMKTDMConfig("CMDM");
		// return TFBUtil.getMKTDMConfig("MKTDMHNCB");
	}

	@Override
	public String getTableName() {
		return DPFTEngine.getSystemProperties("CMDM.cust.cif");
		// return DPFTEngine.getSystemProperties("mktdb.tbl.cust.cont.HNCB");
	}

	@Override
	public String getTableWatchCriteria() throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		sb.append("cust_id in (");
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
	public String getSelectAttrs() {
		return " CUST_ID, BIZ_CAT, CONT_CD, CONT_INFO, CUST_NAME ";
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		/* set db config = XXXXX */
		DPFTConfig config = DPFTUtil.getSystemDBConfig();

		/* Get Data set from "D_LEA" */
		DPFTDboSet dLeaSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_LEA" */
		String qString = DPFTUtil.getFKQueryString(dLeaSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_LEA" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oLeaSet = (DPFTOutboundDboSet) connector.getDboSet("O_LEA", qString);
		oLeaSet.load();
		/* Data set from "O_SSM" should be empty */
		if (oLeaSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oLeaSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		custSet.load();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_LEA", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		//DPFTLogger.info(this, "Flags are " + String.valueOf(dLeaSet.getDbo(0).getString("DECRYPTED_FLAG")) + ", " + String.valueOf(dLeaSet.getDbo(0).getString("PHONE_FLAG")) + ", " + String.valueOf(dLeaSet.getDbo(0).getString("CUST_NAME_FLAG")));
		
		MKTCustContAddOnline addOnline = null;
		for (int i = 0; i < dLeaSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dLeaSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in LEA channel.");
			}
			DPFTDbo dLea = dLeaSet.getDbo(i);
			String cust_id = dLea.getString("customer_id");
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oLeaSet.add();
			new_dbo.setValue(dLeaSet.getDbo(i));
						

			// Decrypted Customer_ID
			if ( ("Y").equals(dLea.getString("DECRYPTED_FLAG")) ) {
				String decryptedID = decryptedIDMap.get(cust_id);
				StringBuilder sbDecrypted = new StringBuilder(decryptedID);
				sbDecrypted.setCharAt(4,'X');
				sbDecrypted.setCharAt(5,'X');
				sbDecrypted.setCharAt(6,'X');
				new_dbo.setValue("DECRYPTED_CUSTOMER_ID", sbDecrypted.toString());
			}
			
			//PHONE
			String mobile = "";
			mobile = custSet.getPrioritizedMobilePhone(cust_id, "LEA",
					GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
			if ( ("Y").equals(dLea.getString("PHONE_FLAG")) ) {
				if (!new_dbo.isNull("PHONE_PRIORITY")) {
					// use mobile_priority Setting
//					mobile = custSet.getPrioritizedMobilePhone(cust_id, "LEA",
//							dLea.getString("PHONE_PRIORITY"));
					if (addOnline == null) {
						addOnline = MKTCustContAddOnline.newInst(getTableWatchCriteria());
					}
					mobile = addOnline.getPrioritizedMobilePhone(cust_id, "LEA", dLea.getString("PHONE_PRIORITY"));
				}
				if (mobile != null && !("").equals(mobile) && !("NA").equals(mobile)) {
					if (mobile.startsWith("+") && mobile.length() > 10) {
						mobile = mobile.replace(mobile.substring(0, mobile.length()-9), "0");
					}
					mobile = mobile.replace("-","");
					if ( mobile.length() == 10 && mobile.startsWith("09") ) {
						StringBuilder sbMobile = new StringBuilder(mobile);
						sbMobile.setCharAt(4,'X');
						sbMobile.setCharAt(5,'X');
						sbMobile.setCharAt(6,'X');
						new_dbo.setValue("PHONE", sbMobile.toString());
					}
				}
			}
			
			//NAME
			//DPFTLogger.info(this, "CUST_NAME_FLAG is  " + String.valueOf(dLea.getString("CUST_NAME_FLAG")));
			if ( ("Y").equals(dLea.getString("CUST_NAME_FLAG")) ) {
				String name = custSet.getName(cust_id);
				DPFTLogger.info(this, "name is  " + String.valueOf(name));
				if (name != null && !("").equals(name) && !("NA").equals(name)) {
					StringBuilder sbName = new StringBuilder(name);
					sbName.setCharAt(1,'O');
					//DPFTLogger.info(this, "sbName is  " + sbName.toString());
					new_dbo.setValue("CUST_NAME", sbName.toString());
				}
			}

			new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
			
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
		DPFTLogger.info(this, "Processed total " + dLeaSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oLeaSet.setRefresh(false);
		oLeaSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oLeaSet, "LEA");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oLeaSet, cell_code_list, cell_name_list, "LEA", true);
		oLeaSet.close();
		dLeaSet.close();
		custSet.close();
		dCustIDSet.close();
		oLeaSet = null;
		dLeaSet = null;
		custSet = null;
		dCustIDSet = null;
		if (addOnline != null) {
			addOnline.close();
		}
		addOnline = null;
		/* Set Result set for next action */
		setResultSet(oLeaSet);
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

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
import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
//import com.ibm.tfb.ext.util.FCBDidSend;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FCBOemActionPersonalDataTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		/* set Table watch db connection properties */
		return TFBUtil.getMKTDMConfig("CMDM");
	}

	@Override
	public String getTableName() {
		return DPFTEngine.getSystemProperties("CMDM.cust.cif");
	}

	@Override
	public String getTableWatchCriteria() throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		sb.append("cust_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(") and CONT_CD = '30'");
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
	/*
	public String send(String cust_id) throws Exception {
		FCBDidSend data = new FCBDidSend();
		data.custId = cust_id;
		DPFTLogger.info(this, "Calling API for: " + data.custId);
		String _DID = data.sendService(data);
		DPFTLogger.info(this, "Calling API and got : " + _DID);
		return _DID;
	}
	*/
	@Override
	public void postAction() throws DPFTRuntimeException {
		/* set db config = MKTDM */
		DPFTConfig config = DPFTUtil.getSystemDBConfig();

		/* Get Data set from previous action */
		DPFTDboSet dOemSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_Channel" */
		String qString = DPFTUtil.getFKQueryString(dOemSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_Channel" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oOemSet = (DPFTOutboundDboSet) connector.getDboSet("O_OEM", qString);
		oOemSet.load();
		/* Data set from "O_Channel" should be empty */
		if (oOemSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oOemSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound
		 * data table
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
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_OEM", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dOemSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
			StringBuilder sbpromotion = new StringBuilder();
			sbpromotion.append("CUST_ID in (");
			sbpromotion.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
					"customer_id"));
			sbpromotion.append(")");
			DPFTDboSet promotionCodeSet = (DPFTDboSet) connector.getDboSet(" CUST_ID, PROMOTION_CODE ", "CMETL.CFMBSEL_STG", sbpromotion.toString());
			DPFTLogger.info(this, "promotionCodeSet DBOSIZEHERE " + String.valueOf(promotionCodeSet.count()));
			for (int i = 0; i < promotionCodeSet.count(); i++) {
				promotionCodeMap.put(promotionCodeSet.getDbo(i).getString("CUST_ID").trim(),
						promotionCodeSet.getDbo(i).getString("PROMOTION_CODE"));
			}
		}
		
		for (int i = 0; i < dOemSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dOemSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in OEM channel.");
			}
			DPFTDbo dOem = dOemSet.getDbo(i);
			String cust_id = dOem.getString("CUSTOMER_ID");
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oOemSet.add();
			new_dbo.setValue(dOemSet.getDbo(i));

			String email = "";
			String email_src = "";
			if ( dOem.getString("email_addr") == null ) {
				if (dOem.isNull("email_priority")) {
					/* use default email priority rule */
					email = custSet.getPrioritizedEmail(cust_id, "EDM",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
				} else {
					/* use email_priority Setting */
					email = custSet.getPrioritizedEmail(cust_id, "EDM", dOem.getString("email_priority"));
				}
				email_src = custSet.getEmail_Src();
				DPFTLogger.debug(this, "email_src = "+email_src);
			}
			else {
				email = dOem.getString("email_addr");
			}
			
			if (email == null || ("").equals(email) || ("NA").equals(email)) {
				// person record doesn't have email info
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
			} else if (email != null) { // edm判斷有無@並排除特殊符號
				if (email.contains("'") || email.contains("\"")) {
					email = email.replaceAll("['\"]", "");
				}
				if (email.contains(";")) {
					email = email.substring(0, email.indexOf(";"));
				}
				if (email.contains("@") && !("").equals(email)) {
					new_dbo.setValue("email_addr", email);
					DPFTLogger.info(this, "MGM_FLAG is : " + dOem.getString("MGM_FLAG"));
					//處理MGM_ID
					String _DID = "";
					if ( dOem.getString("MGM_FLAG").equals("Y")) {
						try {
							//_DID = send(decryptedIDMap.get(cust_id));
							_DID = promotionCodeMap.get(cust_id);
							if ( _DID != null && !("").equals(_DID)) {
								new_dbo.setValue("MGM_ID", _DID);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					} 
					// email source code CF / CP
					if (  !("").equals(email_src) ) {
						if ( email_src.length() == 3 ) {
							email_src = "CF";
						}
						new_dbo.setValue("EMAIL_ADDR_SOURCE_CODE", email_src);
					}
					
					// Name
					if ( dOem.getString("RESV1") == null ) {
						new_dbo.setValue("RESV1", custSet.getName(cust_id));
					}
					
					//String _DID = "";
					//try {
					//	_DID = send(decryptedIDMap.get(cust_id));
					//} catch (Exception e) {
					//	e.printStackTrace();
					//}
					//new_dbo.setValue("MGM_ID", _DID);
					
					new_dbo.setValue("ROW_ID", dOem.getString("treatment_code") + cust_id);
					if ( dOem.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
						new_dbo.setValue("process_status", GlobalConstants.O_DATA_MGM_EXCLUDE);
					}
					else {
						new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
					}
				} else {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
				}
			}
			
			// YYMMDD for file name
			// DateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
			// Date date = new Date();
			// new_dbo.setValue("RESV9", dateformat.format(date).substring(2));
			
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
		DPFTLogger.info(this, "Processed total " + dOemSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oOemSet.setRefresh(false);
		oOemSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oOemSet, "OEM");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oOemSet, cell_code_list, cell_name_list, "OEM", true);
		oOemSet.close();
		dOemSet.close();
		custSet.close();
		dCustIDSet.close();
		oOemSet = null;
		dOemSet = null;
		custSet = null;
		dCustIDSet = null;
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
}

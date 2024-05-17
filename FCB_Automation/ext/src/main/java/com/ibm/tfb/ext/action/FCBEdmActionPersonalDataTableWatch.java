package com.ibm.tfb.ext.action;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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
//import com.ibm.tfb.ext.util.FCBDidSend;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FCBEdmActionPersonalDataTableWatch extends DPFTActionTableWatch {

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
		DPFTDboSet dEdmSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_Channel" */
		String qString = DPFTUtil.getFKQueryString(dEdmSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_Channel" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oEdmSet = (DPFTOutboundDboSet) connector.getDboSet("O_EDM", qString);
		oEdmSet.load();
		/* Data set from "O_Channel" should be empty */
		if (oEdmSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oEdmSet.deleteAll();
		}
		
		// Query CampaignCode and OfferCode and TemplateID from O_EDM
		String confirmFlag = "Y";
		if ( dEdmSet.getDbo(0).getString("ROUTINEFLAG").equals("Y") ) {
			String cc = dEdmSet.getDbo(0).getString("CAMP_CODE");
			String t = dEdmSet.getDbo(0).getString("TEMPLATE_ID");
			String o = dEdmSet.getDbo(0).getString("CAMP_TYPE");
			StringBuilder sb = new StringBuilder();
			sb.append("CAMP_CODE='").append(cc).append("' and TEMPLATE_ID='").append(t).append("' and CAMP_TYPE='").append(o).append("' and PROCESS_STATUS='Output").append("'");
			String qString1 = sb.toString();
			DPFTConnector connector1 = DPFTConnectionFactory.initDPFTConnector(config);
			DPFTOutboundDboSet oEdmHist = (DPFTOutboundDboSet) connector1.getDboSet("O_EDM", qString1);
			oEdmHist.load();
			if (oEdmHist.count() > 0) {
				confirmFlag = "N";
			}
		}

		/*
		 * Validate records with personal info data & add record to outbound
		 * data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		custSet.loadAll();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_EDM", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dEdmSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
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

		MKTCustContAddOnline addOnline = null;
		for (int i = 0; i < dEdmSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dEdmSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in EDM channel.");
			}
			DPFTDbo dEdm = dEdmSet.getDbo(i);
			String cust_id = dEdm.getString("CUSTOMER_ID");
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oEdmSet.add();
			new_dbo.setValue(dEdmSet.getDbo(i));

			// Check if appointment time has expired
			String appointmentTime = dEdm.getString("THIS_STEP_START_DT");
			boolean is_expire = false;
						
			try {
				is_expire = sdf.parse(appointmentTime).before(Calendar.getInstance().getTime());
			} catch (ParseException e) {
				throw new DPFTActionException(this, "SYSTEM", "DPFT0003E", e);
			}
						
			if ( is_expire ) {
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXPIRE_EXCLUDE);
			}
			else {
				String email = "";
				String EMAIL_ADDR_SOURCE_CODE = "";
				if ( dEdm.getString("email_addr") == null ) {
					if (dEdm.isNull("email_priority")) {
						/* use default email priority rule */
						email = custSet.getPrioritizedEmail(cust_id, "EDM",
								GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
						EMAIL_ADDR_SOURCE_CODE = custSet.getEmail_Src();
					} else {
						/* use email_priority Setting */
						//email = custSet.getPrioritizedEmail(cust_id, "EDM", dEdm.getString("email_priority"));
						if (addOnline == null) {
							addOnline = MKTCustContAddOnline.newInst(getTableWatchCriteria());
						}
						email = addOnline.getPrioritizedEmail(cust_id, "EDM", dEdm.getString("email_priority"));
						EMAIL_ADDR_SOURCE_CODE = addOnline.getPrioritized();
					}
				}
				else {
					email = dEdm.getString("email_addr");
					EMAIL_ADDR_SOURCE_CODE = "CM";
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
						//處理MGM_ID
						String _DID = "";
						if ( dEdm.getString("MGM_FLAG").equals("Y")) {
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
						
						String name = "";
						if ( dEdm.getString("name") != null ) {
						}
						else {
							name = custSet.getName(cust_id);
						}
						
						new_dbo.setValue("email_addr", email);
						new_dbo.setValue("EMAIL_ADDR_SOURCE_CODE", EMAIL_ADDR_SOURCE_CODE);
						new_dbo.setValue("name", name);
						new_dbo.setValue("CONFIRMFLAG", confirmFlag);
						if ( dEdm.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
							new_dbo.setValue("process_status", GlobalConstants.O_DATA_MGM_EXCLUDE);
						}
						else {
							new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
						}
					} else {
						new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
					}
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
		TFBUtil.processUsageCode(oEdmSet, "EDM");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oEdmSet, cell_code_list, cell_name_list, "EDM", true);
		oEdmSet.close();
		dEdmSet.close();
		custSet.close();
		dCustIDSet.close();
		oEdmSet = null;
		dEdmSet = null;
		custSet = null;
		dCustIDSet = null;
		if (addOnline != null) {
			addOnline.close();
		}
		addOnline = null;
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

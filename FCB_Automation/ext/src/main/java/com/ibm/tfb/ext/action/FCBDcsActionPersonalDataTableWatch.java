package com.ibm.tfb.ext.action;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
// import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FCBDcsActionPersonalDataTableWatch extends DPFTActionTableWatch {

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
		sb.append("CUST_ID in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(") and CONT_CD = '42'");
		return sb.toString();
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}
	
	@Override
	public String getSelectAttrs() {
		return " CUST_ID, BIZ_CAT, CONT_CD, CONT_INFO";
		//return " CUSTOMER_ID ";
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
		
		/* set db config = XXXXX */
		DPFTConfig config = DPFTUtil.getSystemDBConfig();

		/* Get Data set from "D_DCS" */
		DPFTDboSet dDcsSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_DCS" */
		String qString = DPFTUtil.getFKQueryString(dDcsSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_DCS" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oDcsSet = (DPFTOutboundDboSet) connector.getDboSet("O_DCS", qString);
		oDcsSet.load();
		/* Data set from "O_DCS" should be empty */
		if (oDcsSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oDcsSet.deleteAll();
		}

		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		custSet.load();
		DPFTLogger.info(this, "DBOSIZEHERE " + String.valueOf(custSet.count()));
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_DCS", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dDcsSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
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
		for (int i = 0; i < dDcsSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dDcsSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in DCS channel.");
			}
			DPFTDbo dDcs = dDcsSet.getDbo(i);
			String cust_id = dDcs.getString("customer_id");

			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oDcsSet.add();
			new_dbo.setValue(dDcsSet.getDbo(i));
			
			// Check if appointment time has expired
			String appointmentTime = dDcs.getString("START_TIME") + "00";
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
				//phone
				String mobile = "";
				if ( dDcs.getString("PHONE") == null ) {
					if (new_dbo.isNull("mobile_priority")) {
						/* use default mobile priority rule */
						mobile = custSet.getPrioritizedMobilePhone(cust_id, "DCS",
								GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
					} else {
						/* use mobile_priority Setting */
//						mobile = custSet.getPrioritizedMobilePhone(cust_id, "DCS",
//								dDcs.getString("mobile_priority"));
						if (addOnline == null) {
							addOnline = MKTCustContAddOnline.newInst(getTableWatchCriteria());
						}
						mobile = addOnline.getPrioritizedMobilePhone(cust_id, "DCS", dDcs.getString("mobile_priority"));
					}
				}
				else {
					mobile = dDcs.getString("PHONE");
				}
				
				if (mobile == null || ("").equals(mobile) || ("NA").equals(mobile)) {
					// person record doesn't have mobile info
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
				} else {
					if (!mobile.startsWith("0") && !mobile.startsWith("+")) {
						mobile = "0" + mobile;
					}
					if (mobile.startsWith("+") && mobile.length() > 10) {
						mobile = mobile.replace(mobile.substring(0, mobile.length()-9), "0");
					}
					//解密Customer_ID
					String decrypted_id = decryptedIDMap.get(cust_id);
					
					//處理MGM_ID
					String _DID = "";
					String content = dDcs.getString("CONTENT");
					if ( dDcs.getString("MGM_FLAG").equals("Y") ) {
						try {
							//_DID = send(decryptedIDMap.get(cust_id));
							_DID = promotionCodeMap.get(cust_id);
							if ( _DID != null && !("").equals(_DID)) {
								content = content.replace("{MGMID}", _DID);
								new_dbo.setValue("MGM_ID", _DID);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					
					String regEx = "[~&`^{}\\[\\]|\\\\]";
					Pattern pattern = Pattern.compile(regEx);
					Matcher matcher = pattern.matcher(dDcs.getString("TITLE"));
					StringBuffer sbr = new StringBuffer();
					while (matcher.find()) {
						matcher.appendReplacement(sbr, " ");
					}
					matcher.appendTail(sbr);
					String title = sbr.toString();

					Matcher matcher1 = pattern.matcher(content);
					StringBuffer sbr1 = new StringBuffer();
					while (matcher1.find()) {
						matcher1.appendReplacement(sbr1, " ");
					}
					matcher1.appendTail(sbr1);
					String newContent = sbr1.toString();

					if ( mobile.length() == 10 && mobile.startsWith("09") ) {
						new_dbo.setValue("TITLE", title);
						new_dbo.setValue("CONTENT", newContent);
						new_dbo.setValue("PHONE", mobile);
						new_dbo.setValue("LONG_CUSTOMER_ID", decrypted_id);
						new_dbo.setValue("SHORT_CUSTOMER_ID", decrypted_id.substring(0, 10));
						if ( dDcs.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
							new_dbo.setValue("process_status", GlobalConstants.O_DATA_MGM_EXCLUDE);
						}
						else {
							new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
						}
					}
					else {
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
		DPFTLogger.info(this, "Processed total " + dDcsSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oDcsSet.setRefresh(false);
		oDcsSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oDcsSet, "DCS");

		/* Write results to H_OUTBOUND Table */
		// Daily Output file, don't need to write to H_OUTBOUND
		TFBUtil.generateObndCtrlRecord(connector, oDcsSet, cell_code_list, cell_name_list, "DCS", true);
		dDcsSet.close();
		oDcsSet.close();
		custSet.close();
		dCustIDSet.close();
		dDcsSet = null;
		oDcsSet = null;
		custSet = null;
		dCustIDSet = null;
		if (addOnline != null) {
			addOnline.close();
		}
		addOnline = null;
		/* Set Result set for next action */
		//setResultSet(oDcsSet);
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

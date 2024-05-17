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

public class FCBSmsActionPersonalDataTableWatch extends DPFTActionTableWatch {

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
		sb.append(") and CONT_CD = '42'");
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
		/* set db config = XXXXX */
		DPFTConfig config = DPFTUtil.getSystemDBConfig();

		/* Get Data set from "D_SMS" */
		DPFTDboSet dSmsSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_SMS" */
		String qString = DPFTUtil.getFKQueryString(dSmsSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_SMS" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oSmsSet = (DPFTOutboundDboSet) connector.getDboSet("O_SMS", qString);
		oSmsSet.load();
		/* Data set from "O_SSM" should be empty */
		if (oSmsSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oSmsSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		custSet.load();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_SMS", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dSmsSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
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
		for (int i = 0; i < dSmsSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dSmsSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in SMS channel.");
			}
			DPFTDbo dSms = dSmsSet.getDbo(i);
			String cust_id = dSms.getString("customer_id");
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oSmsSet.add();
			new_dbo.setValue(dSmsSet.getDbo(i));
			
			// Check if appointment time has expired
			StringBuilder sb1 = new StringBuilder();
			sb1.append(dSms.getString("SMSDATE"));
			sb1.append(dSms.getString("SMSTIME"));
			sb1.append("0000");
			String appointmentTime = sb1.toString();
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
				// Decrypt Customer_ID
				String decrypted_id = decryptedIDMap.get(cust_id);
				
				String mobile = "";
				if ( dSms.getString("DA") == null ) {
					if (new_dbo.isNull("phone_priority")) {
						/* use default mobile priority rule */
						mobile = custSet.getPrioritizedMobilePhone(cust_id, "SMS",
								GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
					} else {
						/* use mobile_priority Setting */
//						mobile = custSet.getPrioritizedMobilePhone(cust_id, "SMS",
//								dSms.getString("mobile_priority"));
						if (addOnline == null) {
							addOnline = MKTCustContAddOnline.newInst(getTableWatchCriteria());
						}
						mobile = addOnline.getPrioritizedMobilePhone(cust_id, "SMS", dSms.getString("phone_priority"));
					}
				}
				else {
					mobile = dSms.getString("DA");
				}
				
				if (mobile == null || ("").equals(mobile) || ("NA").equals(mobile)) {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
				} 
				else {
					if (mobile.startsWith("+") && mobile.length() > 10) {
						mobile = mobile.replace(mobile.substring(0, mobile.length()-9), "0");
					}
					mobile = mobile.replace("-","");
					
					if ( dSms.getString("USERNAME") == null ) {
						new_dbo.setValue("USERNAME", custSet.getName(cust_id));
					}
					
					//處理MGM_ID
					String _DID = "";
					if ( dSms.getString("MGM_FLAG").equals("Y") ) {
						try {
							//_DID = send(decryptedIDMap.get(cust_id));
							_DID = promotionCodeMap.get(cust_id);
							if ( _DID != null && !("").equals(_DID)) {
								String content = dSms.getString("SM").replace("{MGMID}", _DID);
								new_dbo.setValue("SM", content);
								new_dbo.setValue("MGM_ID", _DID);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					
					if ( mobile.length() == 10 && mobile.startsWith("09") ) {
						new_dbo.setValue("USERID", decrypted_id);
						new_dbo.setValue("DA", mobile);
						
						// 解密客戶銀行帳號(如果有寫入)
						String encrypted_acc = dSms.getString("ACCNO");
						if ( encrypted_acc != null ) {
							StringBuilder sb3 = new StringBuilder();
							sb3.append("CMDM.ACC_DEC('").append(dSms.getString("Accno")).append("') as D_ACC_NO");
							String dString = sb3.toString();
							DPFTConnector connector2 = DPFTConnectionFactory.initDPFTConnector(config);
							DPFTDboSet dAccIDSet = (DPFTDboSet) connector2.getDboSet(dString, "DUAL", "");
							dAccIDSet.load();
							new_dbo.setValue("ACCNO", dAccIDSet.getDbo(0).getString("D_ACC_NO"));
						}
						if ( dSms.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
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
		DPFTLogger.info(this, "Processed total " + dSmsSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oSmsSet.setRefresh(false);
		oSmsSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oSmsSet, "SMS");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oSmsSet, cell_code_list, cell_name_list, "SMS", false);
		oSmsSet.close();
		dSmsSet.close();
		custSet.close();
		dCustIDSet.close();
		oSmsSet = null;
		dSmsSet = null;
		dCustIDSet = null;
		dCustIDSet = null;
		if (addOnline != null) {
			addOnline.close();
		}
		addOnline = null;
		/* Set Result set for next action */
		setResultSet(oSmsSet);
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

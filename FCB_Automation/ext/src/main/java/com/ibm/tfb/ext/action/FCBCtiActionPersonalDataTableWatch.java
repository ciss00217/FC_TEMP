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
//import com.ibm.tfb.ext.util.FCBDidSend;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FCBCtiActionPersonalDataTableWatch extends DPFTActionTableWatch {

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
		sb.append(")");
		return sb.toString();
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public String getSelectAttrs() {
		return " CUST_ID, BIZ_CAT, CONT_CD, CONT_INFO, CUST_NAME, TO_CHAR(BIRTH_DATE, 'YYYYMMDD') as BIRTH_DATE ";
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
		DPFTDboSet dCtiSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_Channel" */
		String qString = DPFTUtil.getFKQueryString(dCtiSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_Channel" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oCtiSet = (DPFTOutboundDboSet) connector.getDboSet("O_CTI", qString);
		oCtiSet.load();
		/* Data set from "O_Channel" should be empty */
		if (oCtiSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oCtiSet.deleteAll();
		}

		/*
		 * Validate records with personal info data & add record to outbound data table
		 */
		MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		custSet.loadAll();
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_CTI", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dCtiSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
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
		for (int i = 0; i < dCtiSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dCtiSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in CTI channel.");
			}
			String cust_id = dCtiSet.getDbo(i).getString("customer_id");
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oCtiSet.add();
			new_dbo.setValue(dCtiSet.getDbo(i));
			DPFTDbo dCti = dCtiSet.getDbo(i);

			// email
			String[] email = new String[3];
			if ( dCti.getString("CUSTOMER_EMAIL1") == null ) {
				if (dCti.isNull("TEMP_FIELD08")) {
					/* use default email priority rule */
					email = custSet.getAllPrioritizedEmail(cust_id, "CTI", GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
				} else {
					/* use email_priority Setting */
//					email = custSet.getAllPrioritizedEmail(cust_id, "CTI", dCti.getString("TEMP_FIELD08"));
					if (addOnline == null) {
						addOnline = MKTCustContAddOnline.newInst(getTableWatchCriteria());
					}
					email = addOnline.getAllPrioritizedEmail(cust_id, "CTI", dCti.getString("TEMP_FIELD08"));
				}
				new_dbo.setValue("CUSTOMER_EMAIL1", email[0]);
				new_dbo.setValue("CUSTOMER_EMAIL2", email[1]);
			}
			
			// name
			if ( dCti.getString("CUSTOMER_NAME") == null ) {
				new_dbo.setValue("CUSTOMER_NAME", custSet.getName(cust_id));
			}
			
			// birthday
			if ( dCti.getString("CUSTOMER_BIRTHDAY") == null ) {
				new_dbo.setValue("CUSTOMER_BIRTHDAY", custSet.getBDay(cust_id));
			}
			
			// address
			if (dCti.isNull("TEMP_FIELD10")) {
				/* use default email priority rule */
				if ( dCti.getString("CUSTOMER_ADDRESS1") == null ) {
					new_dbo.setValue("CUSTOMER_ADDRESS1", custSet.getPrioritizedResidentAddrWithoutAddrCodeNoRefresh(cust_id, "CTI",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE)[0]);
				}
				if ( dCti.getString("CUSTOMER_ADDRESS2") == null ) {
					new_dbo.setValue("CUSTOMER_ADDRESS2", custSet.getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(cust_id, "CTI",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE)[0]);
				}
			} else {
				/* use email_priority Setting */
				if ( dCti.getString("CUSTOMER_ADDRESS1") == null ) {
					new_dbo.setValue("CUSTOMER_ADDRESS1", custSet.getPrioritizedResidentAddrWithoutAddrCodeNoRefresh(cust_id, "CTI",
							dCti.getString("TEMP_FIELD10"))[0]);
				}
				if ( dCti.getString("CUSTOMER_ADDRESS2") == null ) {
					new_dbo.setValue("CUSTOMER_ADDRESS2", custSet.getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(cust_id, "CTI",
							dCti.getString("TEMP_FIELD10"))[0]);
				}
			}

			// phone
			String[] mobile = new String[3];
			String companyPhone = "";
			String homePhone = "";
			if (new_dbo.isNull("TEMP_FIELD09")) {
				/* use default mobile priority rule */
				mobile = custSet.getAllPrioritizedMobilePhone(cust_id, "CTI", GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
				homePhone = custSet.getPrioritizedHomePhoneNoRefresh(cust_id, "CTI", GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
				companyPhone = custSet.getPrioritizedCompanyPhoneNoRefresh(cust_id, "CTI",
						GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
			} else {
				/* use mobile_priority Setting */
//				mobile = custSet.getAllPrioritizedMobilePhone(cust_id, "CTI", new_dbo.getString("TEMP_FIELD09"));
				if (addOnline == null) {
					addOnline = MKTCustContAddOnline.newInst(getTableWatchCriteria());
				}
				mobile = addOnline.getAllPrioritizedMobilePhone(cust_id, "CTI", dCti.getString("TEMP_FIELD09"));
				homePhone = custSet.getPrioritizedHomePhoneNoRefresh(cust_id, "CTI", new_dbo.getString("TEMP_FIELD09"));
				companyPhone = custSet.getPrioritizedCompanyPhoneNoRefresh(cust_id, "CTI", new_dbo.getString("TEMP_FIELD09"));
			}
			if ( dCti.getString("CUSTOMER_PHONE01") == null ) {
				new_dbo.setValue("CUSTOMER_PHONE01", mobile[0]);
			}
			if ( dCti.getString("CUSTOMER_PHONE04") == null ) {
				new_dbo.setValue("CUSTOMER_PHONE04", mobile[1]);
			}
			if ( dCti.getString("CUSTOMER_PHONE02") == null ) {
				new_dbo.setValue("CUSTOMER_PHONE02", homePhone);
			}
			if ( dCti.getString("CUSTOMER_PHONE03") == null ) {
				new_dbo.setValue("CUSTOMER_PHONE03", companyPhone);
			}

			//解密Customer_ID
			String decrypted_id = decryptedIDMap.get(cust_id);
			new_dbo.setValue("DECRYPTED_CUSTOMER_ID", decrypted_id.substring(0, 10));
			new_dbo.setValue("TEMP_FIELD13", decrypted_id);
			
			//解密客戶銀行帳號(如果有寫入)
			String encrypted_acc = dCti.getString("TEMP_FIELD39");
			if ( encrypted_acc != null ) {
				StringBuilder sb1 = new StringBuilder();
				sb1.append("CMDM.ACC_DEC('").append(dCtiSet.getDbo(i).getString("TEMP_FIELD39")).append("') as D_ACC_NO");
				String dString = sb1.toString();
				DPFTConnector connector2 = DPFTConnectionFactory.initDPFTConnector(config);
				DPFTDboSet dAccIDSet = (DPFTDboSet) connector2.getDboSet(dString, "DUAL", "");
				dAccIDSet.load();
				new_dbo.setValue("CUSTOMER_ACCT_NO", dAccIDSet.getDbo(0).getString("D_ACC_NO"));
			}
			
			//處理MGM_ID
			String _DID = "";
			if ( dCti.getString("MGM_FLAG").equals("Y")) {
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
			
			// at least one phone number is not empty
			if (!(mobile == null & homePhone == null & companyPhone == null)) {
				if ( dCti.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_MGM_EXCLUDE);
				}
				else {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
				}
			} else {
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_EXCLUDE);
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
		DPFTLogger.info(this, "Processed total " + dCtiSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oCtiSet.setRefresh(false);
		oCtiSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oCtiSet, "CTI");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oCtiSet, cell_code_list, cell_name_list, "CTI", true);
		oCtiSet.close();
		dCtiSet.close();
		custSet.close();
		dCustIDSet.close();
		oCtiSet = null;
		dCtiSet = null;
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

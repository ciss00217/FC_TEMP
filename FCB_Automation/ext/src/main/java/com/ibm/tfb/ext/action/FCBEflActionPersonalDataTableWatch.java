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

public class FCBEflActionPersonalDataTableWatch extends DPFTActionTableWatch {

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
		DPFTDboSet dEflSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_Channel" */
		String qString = DPFTUtil.getFKQueryString(dEflSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_Channel" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oEflSet = (DPFTOutboundDboSet) connector.getDboSet("O_EFL", qString);
		oEflSet.load();
		/* Data set from "O_Channel" should be empty */
		if (oEflSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oEflSet.deleteAll();
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
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_EFL", sb.toString());
		DPFTLogger.debug(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		DPFTDboSet dComIDSet = (DPFTDboSet) connector.getDboSet(" SUBSTR(CMDM.ID_DEC(COMPANY_CODE),3,8) AS D_COM_ID, customer_id as CUST_ID ", "D_EFL", sb.toString());
		DPFTLogger.debug(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dComIDSet.count()));
		HashMap<String, String> decryptedComIDMap = new HashMap<String, String>();
		for (int i = 0; i < dComIDSet.count(); i++) {
			decryptedComIDMap.put(dComIDSet.getDbo(i).getString("CUST_ID").trim(),
					dComIDSet.getDbo(i).getString("D_COM_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dEflSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
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
		
		for (int i = 0; i < dEflSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dEflSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in EFL channel.");
			}
			String cust_id = dEflSet.getDbo(i).getString("customer_id");
			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oEflSet.add();
			new_dbo.setValue(dEflSet.getDbo(i));

			DPFTDbo dEfl = dEflSet.getDbo(i);
			
			//email
			String email = "";
			if ( dEfl.getString("EMAIL") == null ) {
				if (dEfl.isNull("email_priority")) {
					/* use default email priority rule */
					email = custSet.getPrioritizedEmailNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE);
				} else {
					/* use email_priority Setting */
					email = custSet.getPrioritizedEmailNoRefresh(cust_id, "EFL", dEfl.getString("email_priority"));
				}
				new_dbo.setValue("EMAIL", email);
			}
			
			//name
			if ( dEfl.getString("NAME") == null ) {
				new_dbo.setValue("NAME", custSet.getName(cust_id));
			}
			
			//birthday
			if ( dEfl.getString("BIRTHDAY") == null ) {
				new_dbo.setValue("BIRTHDAY", custSet.getBDay(cust_id));
			}
			
			//address residentAddr為戶籍地址 houseAddr為通訊地址
			if (dEfl.isNull("address_priority")) {
				/* use default email priority rule */
				if ( dEfl.getString("ADDRESS") == null ) {
					new_dbo.setValue("ADDRESS", custSet.getPrioritizedResidentAddrWithoutAddrCodeNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE)[0]);
				}
				if ( dEfl.getString("ZIPCODE") == null ) {
					new_dbo.setValue("ZIPCODE", custSet.getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE)[1]);
				}
				if ( dEfl.getString("BILL_ADDRESS") == null ) {
					new_dbo.setValue("BILL_ADDRESS", custSet.getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE)[0]);
				}
			} else {
				/* use email_priority Setting */
				if ( dEfl.getString("ADDRESS") == null ) {
					new_dbo.setValue("ADDRESS", custSet.getPrioritizedResidentAddrWithoutAddrCodeNoRefresh(cust_id, "EFL",
							dEfl.getString("address_priority"))[0]);
				}
				if ( dEfl.getString("ZIPCODE") == null ) {
					new_dbo.setValue("ZIPCODE", custSet.getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(cust_id, "EFL",
							dEfl.getString("address_priority"))[1]);
				}
				if ( dEfl.getString("BILL_ADDRESS") == null ) {
					new_dbo.setValue("BILL_ADDRESS", custSet.getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(cust_id, "EFL",
							dEfl.getString("address_priority"))[0]);
				}
			}
			
			//phone
			if (new_dbo.isNull("mobile_priority")) {
				/* use default mobile priority rule */
				if ( dEfl.getString("PHONE") == null ) {
					new_dbo.setValue("PHONE", custSet.getPrioritizedMobilePhoneNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE));
				}
				if ( dEfl.getString("HOME_PHONE") == null ) {
					new_dbo.setValue("HOME_PHONE", custSet.getPrioritizedHomePhoneNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE));
				}
				if ( dEfl.getString("COMPANY_PHONE") == null ) {
					new_dbo.setValue("COMPANY_PHONE", custSet.getPrioritizedCompanyPhoneNoRefresh(cust_id, "EFL",
							GlobalConstants.DPFT_DEFAULT_PRIORITY_CODE));
				}
			} else {
				/* use mobile_priority Setting */
				if ( dEfl.getString("PHONE") == null ) {
					new_dbo.setValue("PHONE", custSet.getPrioritizedMobilePhoneNoRefresh(cust_id, "EFL",
							new_dbo.getString("mobile_priority")));
				}
				if ( dEfl.getString("HOME_PHONE") == null ) {
					new_dbo.setValue("HOME_PHONE", custSet.getPrioritizedHomePhoneNoRefresh(cust_id, "EFL",
							new_dbo.getString("mobile_priority")));
				}
				if ( dEfl.getString("COMPANY_PHONE") == null ) {
					new_dbo.setValue("COMPANY_PHONE", custSet.getPrioritizedCompanyPhoneNoRefresh(cust_id, "EFL",
							new_dbo.getString("mobile_priority")));
				}
			}
			
			String decrypted_id = decryptedIDMap.get(cust_id);
			new_dbo.setValue("DECRYPTED_CUSTOMER_ID", decrypted_id);
			
			String decrypted_com_id = decryptedComIDMap.get(cust_id);
			new_dbo.setValue("COMPANY_CODE", decrypted_com_id);
			
			//處理MGM_ID
			String _DID = "";
			if ( dEfl.getString("MGM_FLAG").equals("Y")) {
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
			
			if ( dEfl.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
				new_dbo.setValue("process_status", GlobalConstants.O_DATA_MGM_EXCLUDE);
			}
			else {
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
		DPFTLogger.info(this, "Processed total " + dEflSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oEflSet.setRefresh(false);
		oEflSet.save();

		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oEflSet, "EFL");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oEflSet, cell_code_list, cell_name_list, "EFL", true);
		oEflSet.close();
		dEflSet.close();
		dCustIDSet.close();
		custSet.close();
		oEflSet = null;
		dEflSet = null;
		dCustIDSet = null;
		custSet = null;
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

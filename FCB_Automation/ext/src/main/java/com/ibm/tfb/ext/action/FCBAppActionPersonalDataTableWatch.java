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
//import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
//import com.ibm.tfb.ext.util.FCBDidSend;
// import com.ibm.tfb.ext.dbo.MKTDMCustomerContactDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FCBAppActionPersonalDataTableWatch extends DPFTActionTableWatch {

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
		sb.append(")");
		return sb.toString();
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}
	
	@Override
	public String getSelectAttrs() {
		return null;
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

		/* Get Data set from "D_APP" */
		DPFTDboSet dAppSet = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();

		/* Set Query criteria for "O_APP" */
		String qString = DPFTUtil.getFKQueryString(dAppSet.getDbo(0));
		if (qString == null) {
			DPFTLogger.debug(this, "Built FK Query String Failed...");
			return;
		}

		/* Get Data set from "O_APP" */
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTOutboundDboSet oAppSet = (DPFTOutboundDboSet) connector.getDboSet("O_APP", qString);
		oAppSet.load();
		/* Data set from "O_APP" should be empty */
		if (oAppSet.count() > 0) {
			DPFTLogger.info(this, "Records exist in output data set...Delete All Records...");
			oAppSet.deleteAll();
		}

		//MKTDMCustomerContactDboSet custSet = (MKTDMCustomerContactDboSet) this.getDataSet();
		//custSet.load();
		//DPFTLogger.info(this, "DBOSIZEHERE " + String.valueOf(custSet.count()));
		ArrayList<String> cell_code_list = new ArrayList<String>();
		ArrayList<String> cell_name_list = new ArrayList<String>();
		long ps_start_time = System.currentTimeMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		StringBuilder sb = new StringBuilder();
		sb.append("customer_id in (");
		sb.append(TFBUtil.getCustomerSelectINString(((DPFTActionTableWatch) this.getPreviousAction()).getDataSet(),
				"customer_id"));
		sb.append(")");
		DPFTDboSet dCustIDSet = (DPFTDboSet) connector.getDboSet(" CMDM.ID_DEC(customer_id) AS D_CUST_ID, customer_id as CUST_ID ", "D_APP", sb.toString());
		DPFTLogger.info(this, "dCustIDSet DBOSIZEHERE " + String.valueOf(dCustIDSet.count()));
		HashMap<String, String> decryptedIDMap = new HashMap<String, String>();
		for (int i = 0; i < dCustIDSet.count(); i++) {
			decryptedIDMap.put(dCustIDSet.getDbo(i).getString("CUST_ID").trim(),
					dCustIDSet.getDbo(i).getString("D_CUST_ID"));
		}
		
		HashMap<String, String> promotionCodeMap = new HashMap<String, String>();
		if (dAppSet.getDbo(0).getString("MGM_FLAG").equals("Y")) {
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
		
		for (int i = 0; i < dAppSet.count(); i++) {
			if ( i == 0 | i%10000 == 0 | i == dAppSet.count()-1 ) {
				DPFTLogger.info(this, "Successfully processed " + String.valueOf(i) + " IDs in APP channel.");
			}
			DPFTDbo dApp = dAppSet.getDbo(i);
			String cust_id = dApp.getString("customer_id");

			DPFTOutboundDbo new_dbo = (DPFTOutboundDbo) oAppSet.add();
			new_dbo.setValue(dAppSet.getDbo(i));

			// Check if appointment time has expired
			String appointmentTime = dApp.getString("APPOINTMENT_TIME");
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
				//解密Customer_ID
				String decrypted_id = decryptedIDMap.get(cust_id);
				
				//處理MGM_ID
				String _DID = "";
				if ( dApp.getString("MGM_FLAG").equals("Y")) {
					try {
						//_DID = send(decryptedIDMap.get(cust_id));
						_DID = promotionCodeMap.get(cust_id);
						if ( _DID != null && !("").equals(_DID)) {
							String title = dApp.getString("TITLE").replace("{MGMID}", _DID);
							String content = dApp.getString("CONTENT").replace("{MGMID}", _DID);
							new_dbo.setValue("TITLE", title);
							new_dbo.setValue("CONTENT", content);
							new_dbo.setValue("MGM_ID", _DID);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				} 

				new_dbo.setValue("RECEIVER", decrypted_id.substring(0, 10));
				StringBuilder sb1 = new StringBuilder();
				sb1.append("\"{\"\"CampaignCode\"\":\"\"");
				sb1.append(new_dbo.getString("CAMP_CODE"));
				sb1.append("\"\",\"\"TreatmentCode\"\":\"\"");
				sb1.append(new_dbo.getString("TREATMENT_CODE"));
				sb1.append("\"\",\"\"CellCode\"\":\"\"");
				sb1.append(new_dbo.getString("CELL_CODE"));
				sb1.append("\"\",\"\"Customer_ID\"\":\"\"");
				sb1.append(decrypted_id);
				sb1.append("\"\"}\"");
				new_dbo.setValue("EXTRAMSG", sb1.toString());
				if ( dApp.getString("MGM_FLAG").equals("Y") && (_DID == null || ("").equals(_DID)) ) {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_MGM_EXCLUDE);
				}
				else {
					new_dbo.setValue("process_status", GlobalConstants.O_DATA_OUTPUT);
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
		DPFTLogger.info(this, "Processed total " + dAppSet.count() + ", process time = "
				+ (ps_fin_time - ps_start_time) / 60000 + " min.");
		oAppSet.setRefresh(false);
		oAppSet.save();
		DPFTLogger.info(this, "Processed oAppSet total finish.");
		/* Write usage codes to O_USAGECODE Table */
		TFBUtil.processUsageCode(oAppSet, "APP");

		/* Write results to H_OUTBOUND Table */
		TFBUtil.generateObndCtrlRecord(connector, oAppSet, cell_code_list, cell_name_list, "APP", true);
		dAppSet.close();
		oAppSet.close();
		dCustIDSet.close();
		dAppSet = null;
		oAppSet = null;
		dCustIDSet = null;
		/* Set Result set for next action */
		// setResultSet(oAppSet);
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

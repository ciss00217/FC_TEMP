package com.ibm.tfb.ext.action;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class FCBRmbActionDataFileOutput extends DPFTActionDataFileOutput {
	
	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

	@Override
	public String getChannelName() {
		return "RMB";
	}
	
	@Override
	public void action() throws DPFTRuntimeException {
		DPFTAction prev_action = this.getPreviousAction();
		try {
			if (prev_action instanceof DPFTActionTableWatch) {
				DPFTDboSet rs = prev_action.getResultSet();
		
				DPFTConfig config = DPFTUtil.getDBConfig("MBANK");
				
				DPFTLogger.debug(this, "rs count = " + rs.count());
				
				// Build qString
				String cc = rs.getDbo(0).getString("CAMP_CODE") + rs.getDbo(0).getString("FCB_PROD_CODE");
				String t = rs.getDbo(0).getString("TREATMENT_CD");
				StringBuilder sb = new StringBuilder();
				sb.append("CM_ID='").append(cc).append("' and TREATMENT_CD='").append(t).append("'");
				String qString = sb.toString();
				
				DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
				DPFTDboSet oMBANKListSet = (DPFTDboSet) connector.getDboSet("MBANK_AP.CMLIST_MF", qString);
				oMBANKListSet.load();
				
				ArrayList<String> cell_code_list = new ArrayList<String>();
				ArrayList<String> cell_name_list = new ArrayList<String>();
				
				/* Data set from "oMBANKListSet" should be empty */
				if (oMBANKListSet.count() > 0) {
					DPFTLogger.info(this, "Records exist in MBANK Data Set...Delete All Records...");
					oMBANKListSet.deleteAll();
				}
				
				for (int i = 0; i < rs.count(); i++) {
					if (!rs.getDbo(i).getString("PROCESS_STATUS").equals("Output"))
						continue;
					DPFTDbo new_dbo = (DPFTDbo) oMBANKListSet.add();
					DPFTDbo rsDbo = rs.getDbo(i);
					new_dbo.setValue("CM_ID", rsDbo.getString("CAMP_CODE") + rsDbo.getString("FCB_PROD_CODE"));
					new_dbo.setValue("CUST_ID", rsDbo.getString("DECRYPTED_CUSTOMER_ID"));
					new_dbo.setValue("URL", rsDbo.getString("OFFR_URL"));
					new_dbo.setValue("OFFER", rsDbo.getString("OFFR_NAME"));
					new_dbo.setValue("OFFER_DESC", rsDbo.getString("OFFR_DESC"));
					new_dbo.setValue("TREATMENT_CD", rsDbo.getString("TREATMENT_CODE"));
					new_dbo.setValue("EFF_DATE", rsDbo.getString("OFFR_EFFECTIVEDATE"));
					new_dbo.setValue("END_DATE", rsDbo.getString("OFFR_EXPIRATIONDATE"));
					new_dbo.setValue("RETRIEVE_FLAG", rsDbo.getString("RETRIEVE_FLAG"));
					// find distinct cell code
					if (!cell_code_list.contains(rs.getDbo(i).getString("cell_code"))) {
						cell_code_list.add(rs.getDbo(i).getString("cell_code"));
					}
					if (!cell_name_list.contains(rs.getDbo(i).getString("cellname"))) {
						cell_name_list.add(rs.getDbo(i).getString("cellname"));
					}
				}
			oMBANKListSet.setRefresh(false);
			oMBANKListSet.save();
			oMBANKListSet.close();
			this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
			} 
		}catch (Exception e) {
			DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
			ex.handleException();
		}
	}

}

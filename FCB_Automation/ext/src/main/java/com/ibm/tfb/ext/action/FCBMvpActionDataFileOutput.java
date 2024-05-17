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
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;

public class FCBMvpActionDataFileOutput extends DPFTActionDataFileOutput {

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

	@Override
	public String getChannelName() {
		return "MVP";
	}

	@Override
	public void action() throws DPFTRuntimeException {
		DPFTAction prev_action = this.getPreviousAction();
		try {
			if (prev_action instanceof DPFTActionTableWatch) {
				DPFTDboSet rs = prev_action.getResultSet();

				DPFTConfig config = DPFTUtil.getDBConfig("MTSFA");

				DPFTLogger.debug(this, "rs count = " + rs.count());

				// Build qString
				String cc = rs.getDbo(0).getString("CAMPAIGN_ID");
				String t = rs.getDbo(0).getString("STEP_ID");
				StringBuilder sb = new StringBuilder();
				sb.append("CAMPAIGN_ID='").append(cc).append("' and STEP_ID='").append(t).append("'");
				String qString = sb.toString();

				DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
				DPFTDboSet oMTSFACampaignSet = (DPFTDboSet) connector.getDboSet("MTSFA.CM_MVP_CAMPAIGN", qString);
				oMTSFACampaignSet.load();

				/* Data set from "oMTSFASet" should be empty */
				if (oMTSFACampaignSet.count() > 0) {
					DPFTLogger.info(this, "Records exist in MTSFA CAMPAIGN Data Set...Delete All Records...");
					oMTSFACampaignSet.deleteAll();
				}

				// Write 1 record to MTSFA.CM_MVP_CAMPAIGN
				DPFTDbo new_dbo = (DPFTDbo) oMTSFACampaignSet.add();
				new_dbo.setValue("CAMPAIGN_ID", rs.getDbo(0).getString("CAMP_CODE"));
				new_dbo.setValue("STEP_ID", rs.getDbo(0).getString("STEP_ID"));
				new_dbo.setValue("CAMPAIGN_NAME", rs.getDbo(0).getString("CAMPAIGNNAME"));
				new_dbo.setValue("SALES_PITCH", rs.getDbo(0).getString("SALES_PITCH"));
				new_dbo.setValue("LEAD_SOURCE_ID", rs.getDbo(0).getString("LEAD_SOURCE_ID"));
				new_dbo.setValue("CAMP_TYPE", rs.getDbo(0).getString("MVP_CAMP_TYPE"));
				new_dbo.setValue("START_DATE", rs.getDbo(0).getString("START_DATE"));
				new_dbo.setValue("END_DATE", rs.getDbo(0).getString("END_DATE"));
				new_dbo.setValue("CAM_EXEC_END_DT", rs.getDbo(0).getString("CAM_EXEC_END_DT"));
				new_dbo.setValue("LEAD_RESPONSE_CODE", rs.getDbo(0).getString("LEAD_RESPONSE_CODE"));
				new_dbo.setValue("LEAD_TYPE", rs.getDbo(0).getString("MVP_LEAD_TYPE"));
				new_dbo.setValue("CHANNEL", rs.getDbo(0).getString("MVP_CHANNEL"));
				new_dbo.setValue("CUST_REPLY_FLAG", rs.getDbo(0).getString("CUST_REPLY_FLAG"));
				new_dbo.setValue("MARKETING_FLAG", rs.getDbo(0).getString("MARKETING_FLAG"));
				new_dbo.setValue("CAM_DISP_ROLE", rs.getDbo(0).getString("CAM_DISP_ROLE"));
				new_dbo.setValue("AUTO_DISPATCH_FLAG", rs.getDbo(0).getString("AUTO_DISPATCH_FLAG"));
				new_dbo.setValue("TERRITORY_ID", rs.getDbo(0).getString("TERRITORY_ID"));
				new_dbo.setValue("KEEP_MONTH", rs.getDbo(0).getString("KEEP_MONTH"));
				new_dbo.setValue("SEND_MAIL", rs.getDbo(0).getString("SEND_MAIL"));
				new_dbo.setValue("CHANNEL_1", rs.getDbo(0).getString("CHANNEL_1"));
				new_dbo.setValue("CHANNEL_2", rs.getDbo(0).getString("CHANNEL_2"));
				new_dbo.setValue("CHANNEL_3", rs.getDbo(0).getString("CHANNEL_3"));

				// TODO 20221208
				new_dbo.setValue("NEED_REVIEW", rs.getDbo(0).getString("NEED_REVIEW"));
				new_dbo.setValue("NSW_MAIL", rs.getDbo(0).getString("NSW_MAIL"));
				new_dbo.setValue("NSM_MAIL", rs.getDbo(0).getString("NSM_MAIL"));

				oMTSFACampaignSet.setRefresh(false);
				oMTSFACampaignSet.save();
				oMTSFACampaignSet.close();

				DPFTDboSet oMTSFALeadSet = (DPFTDboSet) connector.getDboSet("MTSFA.CM_MVP_CAMPAIGN_LEADS", qString);
				oMTSFALeadSet.load();
				ArrayList<String> cell_code_list = new ArrayList<String>();
				ArrayList<String> cell_name_list = new ArrayList<String>();

				/* Data set from "oMTSFALeadSet" should be empty */
				if (oMTSFALeadSet.count() > 0) {
					DPFTLogger.info(this, "Records exist in MTSFA LEADS Data Set...Delete All Records...");
					oMTSFALeadSet.deleteAll();
				}

				for (int i = 0; i < rs.count(); i++) {
					if (!rs.getDbo(i).getString("PROCESS_STATUS").equals("Output"))
						continue;
					DPFTDbo new_lead_dbo = (DPFTDbo) oMTSFALeadSet.add();
					new_lead_dbo.setValue("CAMPAIGN_ID", rs.getDbo(i).getString("CAMP_CODE"));
					new_lead_dbo.setValue("STEP_ID", rs.getDbo(i).getString("STEP_ID"));
					new_lead_dbo.setValue("SFA_LEAD_ID", rs.getDbo(i).getString("TREATMENT_CODE"));
					new_lead_dbo.setValue("CUST_ID", rs.getDbo(i).getString("CUSTOMER_ID"));
					new_lead_dbo.setValue("BRANCH_ID", rs.getDbo(i).getString("BRANCH_ID"));
					new_lead_dbo.setValue("START_DATE", rs.getDbo(i).getString("START_DATE"));
					new_lead_dbo.setValue("END_DATE", rs.getDbo(i).getString("END_DATE"));
					new_lead_dbo.setValue("FIELD1_NAME", rs.getDbo(i).getString("FIELD1_NAME"));
					new_lead_dbo.setValue("FIELD1_VALUE", rs.getDbo(i).getString("FIELD1_VALUE"));
					new_lead_dbo.setValue("FIELD2_NAME", rs.getDbo(i).getString("FIELD2_NAME"));
					new_lead_dbo.setValue("FIELD2_VALUE", rs.getDbo(i).getString("FIELD2_VALUE"));
					new_lead_dbo.setValue("FIELD3_NAME", rs.getDbo(i).getString("FIELD3_NAME"));
					new_lead_dbo.setValue("FIELD3_VALUE", rs.getDbo(i).getString("FIELD3_VALUE"));
					new_lead_dbo.setValue("FIELD4_NAME", rs.getDbo(i).getString("FIELD4_NAME"));
					new_lead_dbo.setValue("FIELD4_VALUE", rs.getDbo(i).getString("FIELD4_VALUE"));
					new_lead_dbo.setValue("FIELD5_NAME", rs.getDbo(i).getString("FIELD5_NAME"));
					new_lead_dbo.setValue("FIELD5_VALUE", rs.getDbo(i).getString("FIELD5_VALUE"));
					new_lead_dbo.setValue("FIELD6_NAME", rs.getDbo(i).getString("FIELD6_NAME"));
					new_lead_dbo.setValue("FIELD6_VALUE", rs.getDbo(i).getString("FIELD6_VALUE"));
					new_lead_dbo.setValue("FIELD7_NAME", rs.getDbo(i).getString("FIELD7_NAME"));
					new_lead_dbo.setValue("FIELD7_VALUE", rs.getDbo(i).getString("FIELD7_VALUE"));
					new_lead_dbo.setValue("FIELD8_NAME", rs.getDbo(i).getString("FIELD8_NAME"));
					new_lead_dbo.setValue("FIELD8_VALUE", rs.getDbo(i).getString("FIELD8_VALUE"));
					new_lead_dbo.setValue("FIELD9_NAME", rs.getDbo(i).getString("FIELD9_NAME"));
					new_lead_dbo.setValue("FIELD9_VALUE", rs.getDbo(i).getString("FIELD9_VALUE"));
					new_lead_dbo.setValue("FIELD10_NAME", rs.getDbo(i).getString("FIELD10_NAME"));
					new_lead_dbo.setValue("FIELD10_VALUE", rs.getDbo(i).getString("FIELD10_VALUE"));
					new_lead_dbo.setValue("FIELD11_NAME", rs.getDbo(i).getString("FIELD11_NAME"));
					new_lead_dbo.setValue("FIELD11_VALUE", rs.getDbo(i).getString("FIELD11_VALUE"));
					new_lead_dbo.setValue("FIELD12_NAME", rs.getDbo(i).getString("FIELD12_NAME"));
					new_lead_dbo.setValue("FIELD12_VALUE", rs.getDbo(i).getString("FIELD12_VALUE"));
					new_lead_dbo.setValue("FIELD13_NAME", rs.getDbo(i).getString("FIELD13_NAME"));
					new_lead_dbo.setValue("FIELD13_VALUE", rs.getDbo(i).getString("FIELD13_VALUE"));
					new_lead_dbo.setValue("FIELD14_NAME", rs.getDbo(i).getString("FIELD14_NAME"));
					new_lead_dbo.setValue("FIELD14_VALUE", rs.getDbo(i).getString("FIELD14_VALUE"));
					new_lead_dbo.setValue("FIELD15_NAME", rs.getDbo(i).getString("FIELD15_NAME"));
					new_lead_dbo.setValue("FIELD15_VALUE", rs.getDbo(i).getString("FIELD15_VALUE"));
					new_lead_dbo.setValue("FIELD16_NAME", rs.getDbo(i).getString("FIELD16_NAME"));
					new_lead_dbo.setValue("FIELD16_VALUE", rs.getDbo(i).getString("FIELD16_VALUE"));
					new_lead_dbo.setValue("FIELD17_NAME", rs.getDbo(i).getString("FIELD17_NAME"));
					new_lead_dbo.setValue("FIELD17_VALUE", rs.getDbo(i).getString("FIELD17_VALUE"));
					new_lead_dbo.setValue("FIELD18_NAME", rs.getDbo(i).getString("FIELD18_NAME"));
					new_lead_dbo.setValue("FIELD18_VALUE", rs.getDbo(i).getString("FIELD18_VALUE"));
					new_lead_dbo.setValue("FIELD19_NAME", rs.getDbo(i).getString("FIELD19_NAME"));
					new_lead_dbo.setValue("FIELD19_VALUE", rs.getDbo(i).getString("FIELD19_VALUE"));
					new_lead_dbo.setValue("FIELD20_NAME", rs.getDbo(i).getString("FIELD20_NAME"));
					new_lead_dbo.setValue("FIELD20_VALUE", rs.getDbo(i).getString("FIELD20_VALUE"));
					// find distinct cell code
					if (!cell_code_list.contains(rs.getDbo(i).getString("cell_code"))) {
						cell_code_list.add(rs.getDbo(i).getString("cell_code"));
					}
					if (!cell_name_list.contains(rs.getDbo(i).getString("cellname"))) {
						cell_name_list.add(rs.getDbo(i).getString("cellname"));
					}
				}
				oMTSFALeadSet.setRefresh(false);
				oMTSFALeadSet.save();
				oMTSFALeadSet.close();
				this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
			}
		} catch (Exception e) {
			DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
			ex.handleException();
		}
	}

}

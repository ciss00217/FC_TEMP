package com.ibm.dpft.engine.core.auto.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTAutomationCCT extends DPFTDbo {

	public DPFTAutomationCCT(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);

	}

	public void setValue(DPFTDbo dbo, String type) throws DPFTRuntimeException {
		this.setValue("taskid", dbo.getString("taskid"));
		this.setValue("name", dbo.getString("name"));
		this.setValue("schedule", dbo.getString("schedule"));
		this.setValue("flowchartid", dbo.getString("flowchartid"));
		this.setValue("filename", dbo.getString("filename"));
		this.setValue("flowcharttype", type);
		this.setValue("addtime", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("campaignid", getCampaignID(dbo.getString("flowchartid")));
	}

	private String getCampaignID(String flowchartid) throws DPFTRuntimeException {
		DPFTDboSet ua_flowchart = DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getDBConfig(GlobalConstants.DB_CONN_PROFILE_UNICADBP))
				.getDboSet("UA_FLOWCHART", "flowchartid='" + flowchartid + "'");
		if (!ua_flowchart.isEmpty()) {
			ua_flowchart.close();
			return ua_flowchart.getDbo(0).getString("CampaignID", false);
		}
		ua_flowchart.close();
		return null;
	}

	public void updateInfo(DPFTDboSet rs) throws DPFTRuntimeException {
		DPFTDbo info = getInfoByFlowchartID(rs);
		if (info == null)
			return;
		this.setValue("CCT_STATUS", info.getString("CCT_STATUS"));
		this.setValue("LOCALUSERNAME", info.getString("LOCALUSERNAME"));
		this.setValue("REMOTEUSERNAME", info.getString("REMOTEUSERNAME"));
		this.setValue("RUNBY", info.getString("RUNBY"));
		this.setValue("RUNENDTIME", info.getString("RUNENDTIME"));
		this.setValue("RUNID", info.getString("RUNID"));
		this.setValue("RUNSTARTTIME", info.getString("RUNSTARTTIME"));
		this.setValue("RUNSTATUS", info.getString("RUNSTATUS"));
		this.setValue("RUNTYPE", info.getString("RUNTYPE"));

		StringBuilder sb = new StringBuilder();
		sb.append("Update CCT Record : \n").append("CCT_STATUS").append("=").append(info.getString("CCT_STATUS"))
				.append("\n").append("LOCALUSERNAME").append("=").append(info.getString("LOCALUSERNAME")).append("\n")
				.append("REMOTEUSERNAME").append("=").append(info.getString("REMOTEUSERNAME")).append("\n")
				.append("RUNBY").append("=").append(info.getString("RUNBY")).append("\n").append("RUNENDTIME")
				.append("=").append(info.getString("RUNENDTIME")).append("\n").append("RUNID").append("=")
				.append(info.getString("RUNID")).append("\n").append("RUNSTARTTIME").append("=")
				.append(info.getString("RUNSTARTTIME")).append("\n").append("RUNSTATUS").append("=")
				.append(info.getString("RUNSTATUS")).append("\n").append("RUNTYPE").append("=")
				.append(info.getString("RUNTYPE")).append("\n");

		DPFTLogger.debug(this, sb.toString());
	}

	private DPFTDbo getInfoByFlowchartID(DPFTDboSet rs) throws DPFTRuntimeException {
		for (int i = 0; i < rs.count(); i++) {
			if (this.getString("flowchartid").equals(rs.getDbo(i).getString("flowchartid")))
				return rs.getDbo(i);
		}
		return null;
	}

}

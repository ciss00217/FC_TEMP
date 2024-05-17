package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTOutboundControlDbo extends DPFTDbo {

	public DPFTOutboundControlDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);
	}

	public void setBasicInfo(DPFTDbo dbo, String out_table, String chal_name) {
		
		this.setValue("camp_code", dbo.getColumnValue("camp_code"));
		this.setValue("timestamp", dbo.getColumnValue("timestamp"));
		this.setValue("target_ds", out_table);
		this.setValue("chal_name", chal_name);
	}

	public void setTotalOutputDataQuantity(int num_output) {
		
		this.setValue("quantity", String.valueOf(num_output));
	}

	public void setTotalExcludeDataQuantity(int num_exclude) {
		
		this.setValue("del_quantity", String.valueOf(num_exclude));
	}

	public String getDataSelectCriteria() {
		String obn_rule = this.getString("obn_rule");
		String tString = this.getString("tstring");
		StringBuilder sb = new StringBuilder();
		if(obn_rule.equals(GlobalConstants.DPFT_TP_OBND_RULE_CAMP)){
			if(this.isNull("camp_code"))
				return null;
			
			String cc = this.getString("camp_code");
			sb.append("camp_code='").append(cc).append("' and ");
		}else if(obn_rule.equals(GlobalConstants.DPFT_TP_OBND_RULE_CELL)){
			if(this.isNull("camp_code") || this.isNull("cell_code"))
				return null;
			
			String cc = this.getString("camp_code");
			String ce = this.getString("cell_code");
			sb.append("camp_code='").append(cc)
			.append("' and cell_code='").append(ce).append("' and ");
		}
		sb.append("timestamp in (" + tString + ")");
		
		DPFTLogger.debug(this, "Outbound data watch criteria : " + sb.toString());
		return sb.toString();
	}

	public void setTotalDataQuantity(DPFTDboSet oSet, String cell_code) throws DPFTRuntimeException {
		int num_exclude = 0;
		int num_output = 0;
		for(int i = 0; i < oSet.count(); i++){
			if(oSet.getDbo(i).tobeDeleted())
				continue;
			
			if(oSet.getDbo(i).getString("cell_code").equals(cell_code) 
					&& oSet.getDbo(i).getString("process_status").equals(GlobalConstants.O_DATA_OUTPUT)){
				num_output++;
			}
			if(oSet.getDbo(i).getString("cell_code").equals(cell_code) 
					&& oSet.getDbo(i).getString("process_status").indexOf(GlobalConstants.O_DATA_EXCLUDE) != -1){
				num_exclude++;
			}
		}
		this.setTotalExcludeDataQuantity(num_exclude);
		this.setTotalOutputDataQuantity(num_output);
	}

	public void complete(String ftp_time) {
		
		this.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_FIN);
		this.setValue("ftp_time", ftp_time);
	}

	public boolean isRunnable() throws DPFTRuntimeException {
		DPFTInboundControlDboSet ibndSet = (DPFTInboundControlDboSet) this.getDboSet("H_INBOUND", "gk_flg='Y' and process_status not in ('" + GlobalConstants.DPFT_CTRL_STAT_COMP 
																						  + "','" + GlobalConstants.DPFT_CTRL_STAT_ERROR + "') and chal_name='" + this.getString("chal_name") + "'");
		ibndSet.load();
		ibndSet.close();
		return ibndSet.isEmpty();
	}

	public void error() {
		this.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_ERROR);
		DPFTLogger.debug(this, "set process_status=" + GlobalConstants.DPFT_OBND_STAT_ERROR);
	}

	public void run() {
		this.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_RUN);
	}

}

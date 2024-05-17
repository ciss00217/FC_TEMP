package com.ibm.tfb.ext.action;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicDataTableWatch;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTResMainDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDbo;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;

public class FCBMvpActionResDataTableWatch extends DPFTActionObndPeriodicDataTableWatch{
	
	@Override
	public DPFTConfig getDBConfig() {
		return TFBUtil.getMTSFAConfig();
	}
	
	@Override
	public String getTableName() {
		return "MTSFA.CM_MVP_LEAD_RESP";
	}
	
	@Override
	public String getTableWatchCriteria() {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		DPFTTriggerMapDefDbo tmap = (DPFTTriggerMapDefDbo) this.getInitialData();
		String t_start = tmap.getString("LAST_ACTIVE_TIME").substring(0,8);
		DPFTLogger.debug(this, "LAST_ACTIVE_TIME is " + tmap.getString("LAST_ACTIVE_TIME"));
		
		Calendar cal = Calendar.getInstance();
		String t_end = sdf.format(cal.getTime());
		//cal.add(Calendar.DAY_OF_MONTH, -1);
		//String t_minus_1 = sdf.format(cal.getTime());
		// cal.add(Calendar.DAY_OF_MONTH, -1);
		// String t_minus_2 = sdf.format(cal.getTime());
		return "RESP_DATE BETWEEN '" + t_start + "' AND '" + t_end + "' and CUST_ID is not null";
	}
	
	@Override
	public String getTriggerKeyCol() {
		return null;
	}
	
	@Override
	public void postAction() throws DPFTRuntimeException{
		DPFTDboSet set = this.getDataSet();
		String time = DPFTUtil.getCurrentTimeStampAsString();
		if(!set.isEmpty()) {
			DPFTResMainDboSet mvpSet = (DPFTResMainDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("RSP_MAIN","chal_name='MVP'");
			mvpSet.setNeedAdd2DelTableFlg(false);
			
			for(int i=0; i<set.count();i++) {
				
				DPFTDbo mvp = mvpSet.add();
				mvp.setValue("TREATMENT_CODE",set.getDbo(i).getString("SFA_LEAD_ID"));
				mvp.setValue("CUSTOMER_ID",set.getDbo(i).getString("CUST_ID"));
				mvp.setValue("RES_DATE",set.getDbo(i).getString("RESP_DATE"));
				mvp.setValue("RES_CODE",set.getDbo(i).getString("RESP_CODE"));
				mvp.setValue("RESV1",set.getDbo(i).getString("RESV1"));
				mvp.setValue("RESV2",set.getDbo(i).getString("RESV2"));
				mvp.setValue("CHAL_NAME","MVP");
				mvp.setValue("ORIG_RESP_CODE",set.getDbo(i).getString("ORIG_RESP_CODE"));
			}
			
			mvpSet.save();
			
			DPFTDboSet hSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("H_INBOUND_RES", "chal_name='MVP'");
			DPFTDbo h = hSet.add();
			h.setValue("chal_name", "MVP");
			h.setValue("process_time", time);
			//h.setValue("d_file", getTableName() + "." + res_date);
			h.setValue("quantity", String.valueOf(mvpSet.getValidResCount()));
			h.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_COMP);
			hSet.save();
			mvpSet.close();
			hSet.close();
			
			DPFTTriggerMapDefDbo tmap = (DPFTTriggerMapDefDbo) this.getInitialData();
			DPFTTriggerMapDefDboSet tmapSet = tmap.getControlTableRecords();
			tmapSet.updateLastActiveTime();
			tmapSet.save();
			tmapSet.close();
		}
		else {
			DPFTDboSet hSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("H_INBOUND_RES", "chal_name='MVP'");
			DPFTDbo h = hSet.add();
			h.setValue("chal_name", "MVP");
			h.setValue("process_time", time);
			//h.setValue("d_file", getTableName() + "." + res_date);
			h.setValue("quantity", "0");
			h.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_COMP);
			hSet.save();
			hSet.close();
			DPFTTriggerMapDefDbo tmap = (DPFTTriggerMapDefDbo) this.getInitialData();
			DPFTTriggerMapDefDboSet tmapSet = tmap.getControlTableRecords();
			tmapSet.updateLastActiveTime();
			tmapSet.save();
			tmapSet.close();
		}
	}
	
	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException{
		//TODO Auto-generated method stub
	}

}

package com.ibm.tfb.ext.action;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTResMainDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;

public class WmsActionResDataWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		return TFBUtil.getMKTDMConfig();
	}

	@Override
	public String getTableName() {
		return "MKT_PS_SA_LEADS_ALL";
	}

	@Override
	public String getTableWatchCriteria() {
		//ROW_LASTMANT_DTTM between to_date('T-2','YYYYMMDD') and to_date('T-1', 'YYYYMMDD');
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DAY_OF_MONTH, -1);
		String t_minus_1 = sdf.format(cal.getTime());
		cal.add(Calendar.DAY_OF_MONTH, -1);
		String t_minus_2 = sdf.format(cal.getTime());
		return "ROW_LASTMANT_DTTM >= to_date('" + t_minus_2 + "','YYYYMMDD') and ROW_LASTMANT_DTTM < to_date('" + t_minus_1 + "', 'YYYYMMDD') and actionplan_id is not null";
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		DPFTDboSet set = this.getDataSet();
		if(!set.isEmpty()){
			String time = DPFTUtil.getCurrentTimeStampAsString();
			DPFTResMainDboSet rspSet = (DPFTResMainDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("RSP_MAIN", "chal_name='WMS'");
			rspSet.setNeedAdd2DelTableFlg(false);
			
			HashMap<String, String> tmap = getActionPlanTreatmentCodeMapping();
			ArrayList<String> leads_ids = new ArrayList<String>();
			String res_date = DPFTUtil.convertDateObject2DateString(set.getDbo(0).getDate("row_lastmant_dttm"), GlobalConstants.DFPT_DATETIME_FORMAT);
			for(int i = 0; i < set.count(); i++){
				if(leads_ids.contains(set.getDbo(i).getString("sa_lead_id")))
					continue;
				
				DPFTDbo data = rspSet.add();
				data.setValue("resv1"      , set.getDbo(i).getString("actionplan_id"));
				data.setValue("treatment_code", tmap.get(set.getDbo(i).getString("actionplan_id")));
				data.setValue("customer_id", set.getDbo(i).getString("party_id"));
				data.setValue("res_code"   , set.getDbo(i).getString("le_status"));
				data.setValue("res_date"   , res_date);
				data.setValue("chal_name"  , "WMS");
				data.setValue("process_time", time);
				leads_ids.add(set.getDbo(i).getString("sa_lead_id"));
			}
			rspSet.save();
			
			DPFTDboSet hSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("H_INBOUND_RES", "chal_name='WMS'");
			DPFTDbo h = hSet.add();
			h.setValue("chal_name", "WMS");
			h.setValue("process_time", time);
			h.setValue("d_file", getTableName() + "." + res_date);
			h.setValue("quantity", String.valueOf(rspSet.getValidResCount()));
			h.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_COMP);
			hSet.save();
			rspSet.close();
			hSet.close();
			tmap.clear();
			leads_ids.clear();
		}

	}

	private HashMap<String, String> getActionPlanTreatmentCodeMapping() throws DPFTRuntimeException {
		HashMap<String, String> map = new HashMap<String, String>();
		DPFTDboSet oWmsSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("O_WMS", "process_status='Output'");
		for(int i = 0; i < oWmsSet.count(); i++){
			map.put(oWmsSet.getDbo(i).getString("actionplan_id"), oWmsSet.getDbo(i).getString("treatment_code"));
		}
		oWmsSet.close();
		return map;
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		
		
	}

}

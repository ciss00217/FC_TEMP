package com.ibm.dpft.engine.core.dbo;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTOutboundControlDboSet extends DPFTDboSet {

	
	public DPFTOutboundControlDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTOutboundControlDbo(dboname, d, this);
	}

	public boolean[] getExecTaskPlanIndice(DPFTTriggerMapDefDboSet tmapSet, String tk) throws DPFTRuntimeException {
		int set_cnt = count();
		boolean[] rtn_indx = new boolean[set_cnt];
		ArrayList<String> obnd_cmp_list = new ArrayList<String>();
		ArrayList<String> obnd_cell_list = new ArrayList<String>();
		ArrayList<String> obnd_tp_list = new ArrayList<String>();	
		HashMap<String, String> cmp_t_map = new HashMap<String, String>();
		
		for(int i = 0; i < set_cnt; i++){
			DPFTDbo dbo = this.getDbo(i);
			String tpid = tmapSet.getTpTriggerMap().get(dbo.getString(tk));
			String obn_rule = tmapSet.getObnRule(tpid);
			String cmp_code = dbo.getString("camp_code");
			String cell = dbo.getString("cell_code");
			String t = dbo.getString("timestamp");
			if(obn_rule == null)
				//Default set Outbound rule to : batch file by Campaign Code
				obn_rule = GlobalConstants.DPFT_TP_OBND_RULE_CAMP;
			
			if(obn_rule.equals(GlobalConstants.DPFT_TP_OBND_RULE_CAMP)){
				/*identical campaign code for executable taskplans*/
				String key = tpid + cmp_code;
				if(!obnd_cmp_list.contains(key)){
					rtn_indx[i] = true;
					obnd_cmp_list.add(key);
					cmp_t_map.put(key, "'" + t + "'");
				}else{
					rtn_indx[i] = false;
					String tString = cmp_t_map.get(key);
					if(tString.indexOf(t) == -1){
						//new timestamp
						cmp_t_map.put(key, tString + ",'" + t + "'");
					}
				}
			}else if(obn_rule.equals(GlobalConstants.DPFT_TP_OBND_RULE_CELL)){
				/*identical (campaign code + cell code) for executable taskplans*/
				String key = tpid + cmp_code + cell;
				if(!obnd_cell_list.contains(key)){
					rtn_indx[i] = true;
					obnd_cell_list.add(key);
					cmp_t_map.put(key, "'" + t + "'");
				}else{
					rtn_indx[i] = false;
					String tString = cmp_t_map.get(key);
					if(tString.indexOf(t) == -1){
						//new timestamp
						cmp_t_map.put(key, tString + ",'" + t + "'");
					}
				}
			}else{
				/*identical (tpid) for executable taskplans*/
				String key = tpid;
				if(!obnd_tp_list.contains(key)){
					rtn_indx[i] = true;
					obnd_tp_list.add(key);
					cmp_t_map.put(key, "'" + t + "'");
				}else{
					rtn_indx[i] = false;
					String tString = cmp_t_map.get(key);
					if(tString.indexOf(t) == -1){
						//new timestamp
						cmp_t_map.put(key, tString + ",'" + t + "'");
					}
				}
			}
			dbo.setVirtualData("obn_rule", obn_rule);
		}
		
		//get timestamp info
		for(int i = 0; i < set_cnt; i++){
			DPFTDbo dbo = this.getDbo(i);
			String tpid = tmapSet.getTpTriggerMap().get(dbo.getString(tk));
			String cmp_code = dbo.getString("camp_code");
			String cell = dbo.getString("cell_code");
			if(rtn_indx[i]){
				if(dbo.getString("obn_rule").equals(GlobalConstants.DPFT_TP_OBND_RULE_CAMP))
					dbo.setVirtualData("tstring", cmp_t_map.get(tpid+cmp_code));
				else if(dbo.getString("obn_rule").equals(GlobalConstants.DPFT_TP_OBND_RULE_CELL))
					dbo.setVirtualData("tstring", cmp_t_map.get(tpid+cmp_code+cell));
				else
					dbo.setVirtualData("tstring", cmp_t_map.get(tpid));
						
				
			}
		}
		return rtn_indx;
	}

	public void taskComplete() throws DPFTRuntimeException {
		String ftp_time = DPFTUtil.getCurrentTimeStampAsString();
		for(int i = 0; i < count(); i++){
			((DPFTOutboundControlDbo)this.getDbo(i)).complete(ftp_time);
		}
		save();
	}
	
	@Override
	public DPFTDbo add() throws DPFTRuntimeException {
		DPFTDbo new_dbo = super.add();
		
		//init value
		new_dbo.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_STAGE);
		return new_dbo;
	}

	public void error() throws DPFTRuntimeException {
		for(int i = 0; i < count(); i++){
			((DPFTOutboundControlDbo)this.getDbo(i)).error();
		}
		save();
	}

	public void run() throws DPFTRuntimeException {
		for(int i = 0; i < count(); i++){
			((DPFTOutboundControlDbo)this.getDbo(i)).run();
		}
		save();
	}

}

package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTTriggerMapDefDboSet extends DPFTDboSet {
	public DPFTTriggerMapDefDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	private HashMap<String, String> trigger_map = null;
	
	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTTriggerMapDefDbo(dboname, d, this);
	}
	
	@Override
	public void load() throws DPFTRuntimeException {
		super.load();
		trigger_map = getTpTriggerMap();
	}

	public HashMap<String, String> getTpTriggerMap() throws DPFTRuntimeException {
		if(trigger_map != null)
			return trigger_map;
		
		trigger_map = new HashMap<String, String>();
		for(int i = 0; i < this.count(); i++){
			trigger_map.put(this.getDbo(i).getString("chal_name"), this.getDbo(i).getString("planid"));
		}
		return trigger_map;
	}

	public String getObnRule(String tpid) throws DPFTRuntimeException {
		for(int i = 0; i < this.count(); i++){
			if(this.getDbo(i).getString("planid").equals(tpid))
				return this.getDbo(i).getString("obnd_rule");
		}
		return null;
	}

	public void updateLastActiveTime() throws DPFTRuntimeException {
		DPFTTriggerMapDefDbo parent = (DPFTTriggerMapDefDbo) this.getParent();
		for(int i = 0; i < this.count(); i++){
			DPFTLogger.info(this, "Update last active time from " + this.getDbo(i).getString("last_active_time") + " to " + parent.getTrgTimeString());
			this.getDbo(i).setValue("last_active_time", parent.getTrgTimeString());
			this.getDbo(i).setValue("active", "1");
		}
	}
}

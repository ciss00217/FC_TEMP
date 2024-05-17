package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTPrioritySettingDboSet extends DPFTDboSet {

	
	public DPFTPrioritySettingDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTPrioritySettingDbo(dboname, d, this);
	}

	public DPFTPrioritySettingDbo getPrioritySetting(String template, String p_code) throws DPFTRuntimeException {
		
		for(int i = 0; i < count(); i++){
			if(this.getDbo(i).getString("template").equalsIgnoreCase(template)
					&& this.getDbo(i).getString("priority_code").equalsIgnoreCase(p_code))
				return (DPFTPrioritySettingDbo) this.getDbo(i);
		}
		return null;
	}

}

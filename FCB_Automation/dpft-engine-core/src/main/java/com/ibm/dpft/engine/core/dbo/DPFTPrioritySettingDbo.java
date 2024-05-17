package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

public class DPFTPrioritySettingDbo extends DPFTDbo {

	public DPFTPrioritySettingDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);
		
	}

	public String[] getPrioritySettings() {
		String[] p = new String[3];
		p[0] = this.getString("p1");
		p[1] = this.getString("p2");
		p[2] = this.getString("p3");
		return p;
	}

}

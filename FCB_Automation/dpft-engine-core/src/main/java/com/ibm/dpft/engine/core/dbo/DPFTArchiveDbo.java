package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

public class DPFTArchiveDbo extends DPFTDbo {

	public DPFTArchiveDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);
		
	}

	public void setArchiveInfo(DPFTDbo dbo, String timestamp) {
		this.setValue(dbo);
		this.setValue("archive_time", timestamp);
	}

}

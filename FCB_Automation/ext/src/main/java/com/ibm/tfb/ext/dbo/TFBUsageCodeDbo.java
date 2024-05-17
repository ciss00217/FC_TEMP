package com.ibm.tfb.ext.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;

public class TFBUsageCodeDbo extends DPFTDbo {

	public TFBUsageCodeDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);
		
	}

	public void setInitialData(DPFTDbo dbo, String chal_name, String usagecode) {
		
		this.setValue("chal_name", chal_name);
		this.setValue("camp_code", dbo.getString("camp_code"));
		this.setValue("timestamp", dbo.getString("timestamp"));
		this.setValue("cell_code", dbo.getString("cell_code"));
		this.setValue("customer_id", dbo.getString("customer_id"));
		this.setValue("usagecode", usagecode);
		this.setValue("process_status", dbo.getString("process_status"));
		
	}

}

package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTOutboundDbo extends DPFTDbo {

	public DPFTOutboundDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);

	}

	public void validateWithGateKeeper(HashMap<String, Boolean> gkresult) {

		String cust_id = this.getString("customer_id");
		if (!StringUtils.isEmpty(cust_id)) {
			if (cust_id.contains("||")) {
				cust_id = cust_id.split("||")[0];
			}
		}
		String treatment_code = this.getString("treatment_code");
		String key = cust_id + treatment_code;
		if (gkresult.get(key) != null) {
			DPFTLogger.debug(this, key + " GK_PASS");
			return;
		}
		//DPFTLogger.info(this, key + " GK_EXCLUDE");
		this.setValue("process_status", GlobalConstants.O_DATA_GK_EXCLUDE);
	}

}

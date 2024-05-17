package com.ibm.tfb.ext.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class MKTDMCustomerContactDbo extends DPFTDbo {

	public MKTDMCustomerContactDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);

	}

	public boolean find(String cust_id, String cd) {
		return (this.getString("CUSTOMER_ID").trim().equalsIgnoreCase(cust_id));
	}

	public boolean find(String cust_id, String cont_cd, String biz_type) {

		// return find(cust_id, cont_cd) &&
		// this.getString("biz_cat").equalsIgnoreCase(biz_type);
		//DPFTLogger.info(this, "BIZ_TYPE equals:"+this.getString(DPFTEngine.getSystemProperties("mkt.cont.type")).equalsIgnoreCase(biz_type));
		return find(cust_id, cont_cd)
				&& this.getString(DPFTEngine.getSystemProperties("mkt.cont.type")).equalsIgnoreCase(biz_type);
	}

}

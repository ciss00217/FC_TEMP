package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class UnicaCampaignDbo extends DPFTDbo {

	public UnicaCampaignDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);

	}

	public String getOwnerEmail() throws DPFTRuntimeException {
		DPFTDboSet mailSet = DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getDBConfig(GlobalConstants.DB_CONN_PROFILE_UNICAMPP))
				.getDboSet("USM_USER", "id='" + this.getString("CreateBy", false) + "'");
		if (!mailSet.isEmpty()) {
			mailSet.close();
			return mailSet.getDbo(0).getString("email");
		}
		mailSet.close();
		return null;
	}

}

package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class SystemPropDboSet extends DPFTDboSet {

	public SystemPropDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new SystemPropDbo(dboname, d, this);
	}

	public String getPropValue(String prop) throws DPFTRuntimeException {
		for(int i = 0; i < this.count(); i++){
			if(prop.equalsIgnoreCase(this.getDbo(i).getString("prop"))){
				return this.getDbo(i).getString("value");
			}
		}
		return null;
	}
}

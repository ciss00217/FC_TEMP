package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTMessageDboSet extends DPFTDboSet {
	
	public DPFTMessageDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTMessageDbo(dboname, d, this);
	}

}

package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class ResFileDirSettingDboSet extends DPFTDboSet {
	public ResFileDirSettingDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new ResFileDirSettingDbo(dboname, d, this);
	}
}

package com.ibm.dpft.engine.core.util;

import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTMessage extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8775339755545474881L;

	public DPFTMessage(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTMessage(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTMessage(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTMessage(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

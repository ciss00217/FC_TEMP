package com.ibm.tfb.ext.common;

import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class TFBInvalidCustInfoException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4273583497673824289L;

	public TFBInvalidCustInfoException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public TFBInvalidCustInfoException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public TFBInvalidCustInfoException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public TFBInvalidCustInfoException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

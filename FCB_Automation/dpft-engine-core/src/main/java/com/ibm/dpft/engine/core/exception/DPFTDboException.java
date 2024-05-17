package com.ibm.dpft.engine.core.exception;

public class DPFTDboException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6389566914593344859L;

	public DPFTDboException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTDboException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTDboException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTDboException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

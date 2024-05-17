package com.ibm.dpft.engine.core.exception;

public class DPFTAutomationException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3785532951071011386L;

	public DPFTAutomationException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTAutomationException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTAutomationException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTAutomationException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

package com.ibm.dpft.engine.core.exception;

public class DPFTConnectionException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4835015894070997663L;
	
	public DPFTConnectionException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTConnectionException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTConnectionException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTConnectionException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

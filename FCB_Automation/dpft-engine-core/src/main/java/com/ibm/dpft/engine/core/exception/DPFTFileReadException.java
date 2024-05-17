package com.ibm.dpft.engine.core.exception;

public class DPFTFileReadException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3589024364515104122L;

	public DPFTFileReadException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTFileReadException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTFileReadException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTFileReadException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

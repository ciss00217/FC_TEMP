package com.ibm.dpft.engine.core.exception;

public class DPFTDataFormatException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4305036580985739821L;

	public DPFTDataFormatException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTDataFormatException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTDataFormatException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTDataFormatException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

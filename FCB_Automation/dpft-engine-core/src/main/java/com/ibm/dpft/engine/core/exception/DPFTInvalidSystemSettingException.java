package com.ibm.dpft.engine.core.exception;

public class DPFTInvalidSystemSettingException extends DPFTRuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4947908794149057320L;

	
	public DPFTInvalidSystemSettingException(String gid, String msgid, Object[] params) throws DPFTRuntimeException {
		super(gid, msgid, params);
	}
	
	public DPFTInvalidSystemSettingException(String gid, String msgid) throws DPFTRuntimeException {
		super(gid, msgid);
	}
	
	public DPFTInvalidSystemSettingException(String gid, String msgid, Object[] params, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, params, e);
	}
	
	public DPFTInvalidSystemSettingException(String gid, String msgid, Throwable e) throws DPFTRuntimeException {
		super(gid, msgid, e);
	}
}

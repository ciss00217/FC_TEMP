package com.ibm.dpft.engine.core.exception;

import com.ibm.dpft.engine.core.action.DPFTAction;

public class DPFTActionException extends DPFTRuntimeException {
	private DPFTAction thrower = null;

	/**
	 * 
	 */
	private static final long serialVersionUID = -5881807801389076060L;

	public DPFTActionException(){
		
	}
	
	public DPFTActionException(DPFTAction action, String gid, String msgid, Object[] params, Exception e) throws DPFTRuntimeException{
		super(gid, msgid, params, e);
		setThrower(action);
	}
	
	public DPFTActionException(DPFTAction action, String gid, String msgid, Exception e) throws DPFTRuntimeException{
		super(gid, msgid, e);
		setThrower(action);
	}
	
	public DPFTActionException(DPFTAction action, String gid, String msgid) throws DPFTRuntimeException{
		super(gid, msgid);
		setThrower(action);
	}
	
	public DPFTActionException(DPFTAction action, String gid, String msgid, Object[] params) throws DPFTRuntimeException{
		super(gid, msgid, params);
		setThrower(action);
	}

	@Override
	public void handleException() throws DPFTRuntimeException {
		super.handleException();
		if(thrower != null)
			thrower.handleException(this);
	}

	public DPFTAction getThrower() {
		return thrower;
	}

	public void setThrower(DPFTAction thrower) {
		this.thrower = thrower;
	}
}

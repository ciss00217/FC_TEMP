package com.ibm.dpft.engine.core.auto.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcess;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationMacro {
	private String invoker = null;
	private Object[] invoker_args = null;
	private DPFTAutomationProcess process = null;
	private Object data = null;
	
	public int invoke() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, DPFTRuntimeException {
		String method = this.getInvokeMethod();
		Method thisMethod = null;
		if(method == null || method.isEmpty()){
			throw new DPFTAutomationException("SYSTEM", "AUTO0018E");
		}
		try {
			thisMethod = findInvokeMethod(method);
		} catch (NoSuchMethodException e) {
			Object[] params = {this.getClass().getName(), method};
			throw new DPFTAutomationException("SYSTEM", "AUTO0019E", params, e);
		} catch (SecurityException e) {
			throw e;
		}
		return (int) thisMethod.invoke(this, getInvokeMethodArgs());
	}

	public void setInvokeMethod(String action) {
		invoker = action;
	}
	
	public String getInvokeMethod() {
		return invoker;
	}

	public void setInvokeMethodArgs(Object[] args) {
		invoker_args = args;
	}
	
	public Object[] getInvokeMethodArgs(){
		return invoker_args;
	}
	
	protected Method findInvokeMethod(String method) throws NoSuchMethodException, SecurityException {
		return this.getClass().getDeclaredMethod(method, getParameterTypes());
	}
	
	@SuppressWarnings("rawtypes")
	protected Class[] getParameterTypes() {
		if(invoker_args == null)
			return null;
		
		Class[] cs = new Class[invoker_args.length];
		for(int i = 0; i < invoker_args.length; i++){
			cs[i] = invoker_args[i].getClass();
		}
		return cs;
	}

	public void setProcess(DPFTAutomationProcess p) {
		process = p;
	}
	
	public DPFTAutomationProcess getProcess() {
		return process;
	}

	public Object getMacroReturnData() {
		return data;
	}

	public void setMacroReturnData(Object data) {
		this.data = data;
	}
}

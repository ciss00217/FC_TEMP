package com.ibm.dpft.engine.core.exception;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTMessageDboSet;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTRuntimeException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1667458713808009240L;
	private String gid = null;
	private String msgid = null;

	public DPFTRuntimeException(){
		
	}
	
	public DPFTRuntimeException(String gid, String msgid, Object[] params) throws DPFTRuntimeException{
		super(substitueParams(getMessage(gid, msgid), params));
		this.gid = gid;
		this.msgid = msgid;
	}
	
	public DPFTRuntimeException(String gid, String msgid) throws DPFTRuntimeException{
		super(getMessage(gid, msgid));
		this.gid = gid;
		this.msgid = msgid;
	}
	
	public DPFTRuntimeException(String gid, String msgid, Throwable cause) throws DPFTRuntimeException{
		super(getMessage(gid, msgid), cause);
		this.gid = gid;
		this.msgid = msgid;
	}
	
	public DPFTRuntimeException(String gid, String msgid, Object[] params, Throwable cause) throws DPFTRuntimeException{
		super(substitueParams(getMessage(gid, msgid), params), cause);
		this.gid = gid;
		this.msgid = msgid;
	}
	
	public void handleException() throws DPFTRuntimeException{
		DPFTLogger.error(this);
		String notifier = getMsgNotifier();
		if(notifier != null && !notifier.isEmpty())
			DPFTUtil.pushNotification(notifier, getMessage(), getDetailStackTrace());
	}
	
	public String getDetailStackTrace() {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		printStackTrace(pw);
		return sw.toString();
	}

	public String getMsgNotifier() throws DPFTRuntimeException {
		try {
			DPFTMessageDboSet mset = (DPFTMessageDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_MESSAGE", "gid='" + gid + "' and msgid='" + msgid + "'");
			if(!mset.isEmpty()){
				mset.close();
				return mset.getDbo(0).getString("notifier");
			}
			mset.close();
		} catch (DPFTConnectionException e) {
			return null;
		}
		return null;
	}

	private static String getMessage(String gid, String msgid) throws DPFTRuntimeException {
		try {
			DPFTMessageDboSet mset = (DPFTMessageDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_MESSAGE", "gid='" + gid + "' and msgid='" + msgid + "'");
			if(!mset.isEmpty()){
				mset.close();
				return "(" + msgid + ")" + mset.getDbo(0).getString("msg");
			}
			mset.close();
		} catch (DPFTConnectionException e) {
			return "(" + msgid + ")";
		}
		return "(" + msgid + ")";
	}
	
	private static String substitueParams(String message, Object[] params) {
		Pattern pattern = Pattern.compile("\\{\\d\\}");
		Matcher m = pattern.matcher(message);
		while(m.find()){
			String var = m.group(0);
			int index = Integer.valueOf(var.substring(1, var.length()-1));
			if(index < params.length)
				message = message.replace(var, String.valueOf(params[index]));
		}
		return message;
	}
	
	public String getGid(){
		return this.gid;
	}

	public String getMsgId(){
		return this.msgid;
	}
}

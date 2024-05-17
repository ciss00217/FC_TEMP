package com.ibm.dpft.engine.core.util;


import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DPFTLogger {

	public static void info(Object o ,String message) {
		
		Logger logger = LogManager.getLogger(o.getClass());
		logger.info(getTitleMessage(GlobalConstants.MSG_TITLE_01,message));
	}
	
	public static void info(String classname ,String message) {
		
		Logger logger = LogManager.getLogger(classname);
		logger.info(getTitleMessage(GlobalConstants.MSG_TITLE_01,message));
	}
	
	public static void debug(Object o ,String message) {
		
		Logger logger = LogManager.getLogger(o.getClass());
		logger.debug(getTitleMessage(GlobalConstants.MSG_TITLE_01,message));
	}
	
	public static void debug(String classname ,String message) {
		
		Logger logger = LogManager.getLogger(classname);
		logger.debug(getTitleMessage(GlobalConstants.MSG_TITLE_01,message));
	}
	
	public static void error(Object o ,String message) {
		
		Logger logger = LogManager.getLogger(o.getClass());
		logger.error(getTitleMessage(GlobalConstants.MSG_TITLE_01,message));
	}
	
	public static void error(Object o ,String message, Exception e) {
		
		Logger logger = LogManager.getLogger(o.getClass());
		logger.error(getTitleMessage(GlobalConstants.MSG_TITLE_01,message), e);
	}
	
	public static void error(String classname ,String message) {
		
		Logger logger = LogManager.getLogger(classname);
		logger.error(getTitleMessage(GlobalConstants.MSG_TITLE_01,message));
	}

	public static void error(String classname, String message, Exception e) {
		
		Logger logger = LogManager.getLogger(classname);
		logger.error(getTitleMessage(GlobalConstants.MSG_TITLE_01,message), e);
	}

	private static String getTitleMessage(String title, String message) {
		
		if(title == null || title.isEmpty())
			return message;
		
		StringBuilder sb = new StringBuilder();
		sb.append("[").append(title).append("] (Thread Id: ").append(Thread.currentThread().getId() + ") ").append(message);
		return sb.toString();
	}

	public static void error(DPFTRuntimeException e) {
		Logger logger = LogManager.getLogger(e.getClass().getName());
		logger.error(getTitleMessage(GlobalConstants.MSG_TITLE_01, e.getMessage()), e);
	}

}

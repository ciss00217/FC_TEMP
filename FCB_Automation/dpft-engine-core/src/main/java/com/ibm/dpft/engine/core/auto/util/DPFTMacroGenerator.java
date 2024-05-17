package com.ibm.dpft.engine.core.auto.util;

public class DPFTMacroGenerator {

	public static DPFTAutomationMacro initMacroObject(String classname) throws InstantiationException, IllegalAccessException, ClassNotFoundException {		
		return (DPFTAutomationMacro) Class.forName(classname).newInstance();
	}

}

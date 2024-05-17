package com.ibm.dpft.engine.core.auto.dbo;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.auto.util.DPFTAutomationMacro;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationScriptTranslator;
import com.ibm.dpft.engine.core.auto.util.macro.MacroVar;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationVariables extends DPFTDbo {

	public DPFTAutomationVariables(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);
		
	}

	public Object getValue() throws DPFTRuntimeException {
		if(MacroVar.getCacheVar(this.getString("var")) != null)
			return MacroVar.getCacheVar(this.getString("var"));
		
		String value = this.getString("value");
		if(value.indexOf(";") == -1){
			Pattern pattern = Pattern.compile("\\$\\((.*?)\\)");
			Matcher m = pattern.matcher(value);
			if(m.find())
				value = "subVar;" + value;
			else
				return value;
		}
			
		return loadMacroValue(value);
	}

	private Object loadMacroValue(String value) throws DPFTRuntimeException {
		DPFTAutomationScriptTranslator translator = new DPFTAutomationScriptTranslator(this);
		DPFTAutomationMacro macro = translator.translateVarScript(value);
		if(macro == null)
			return value;
		try {
			int rc = macro.invoke();
			if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL){
				return macro.getMacroReturnData();
			}
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			Object[] params = {"變數處理"};
			throw new DPFTAutomationException("SYSTEM", "AUTO0003E", params, e);
		}
		
		return null;
	}

}

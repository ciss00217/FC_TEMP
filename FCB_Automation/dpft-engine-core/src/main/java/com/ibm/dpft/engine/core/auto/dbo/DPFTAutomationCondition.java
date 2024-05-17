package com.ibm.dpft.engine.core.auto.dbo;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import com.ibm.dpft.engine.core.auto.util.DPFTAutomationMacro;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationScriptTranslator;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationCondition extends DPFTDbo {

	public DPFTAutomationCondition(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);
		
	}

	public boolean isConditionMatched() throws DPFTRuntimeException {
		DPFTAutomationScriptTranslator translator = new DPFTAutomationScriptTranslator(this);
		DPFTAutomationMacro macro = translator.translateConditionScript(this.getString("condition"));
		if(macro == null)
			return false;
		
		try {
			int rc = macro.invoke();
			return rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			Object[] params = {"條件式"};
			throw new DPFTAutomationException("SYSTEM", "AUTO0003E", params, e);
		}
	}

}

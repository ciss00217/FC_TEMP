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

public class DPFTAutomationProcess extends DPFTDbo {

	public DPFTAutomationProcess(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);
		
	}

	public int exec() throws DPFTRuntimeException {
		DPFTAutomationScriptTranslator translator = new DPFTAutomationScriptTranslator(this);
		DPFTAutomationMacro macro = translator.translateScript(this.getString("action"), this.getString("macro"), this.getString("argvs"));
		if(macro == null)
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_ERROR;
		try {
			macro.setProcess(this);
			return macro.invoke();
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			Object[] params = {"動作"};
			throw new DPFTAutomationException("SYSTEM", "AUTO0003E", params, e);
		}
	}

	public boolean isRunnable() {
		return this.getString("process_status").equals(GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_RUNNABLE);
	}

	public boolean isFinished() {
		return this.getString("process_status").equals(GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_FIN);
	}

	public boolean isRunning() {
		return this.getString("process_status").equals(GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_RUNNING);
	}
	
	public boolean isError() {
		return this.getString("process_status").equals(GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_ERROR);
	}

}

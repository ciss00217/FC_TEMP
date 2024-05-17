package com.ibm.dpft.engine.core.auto.util.macro;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcessSet;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationMacro;
import com.ibm.dpft.engine.core.auto.util.DPFTExpressionParser;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class MacroCondition extends DPFTAutomationMacro {
	public int processCondition(String condition) throws DPFTRuntimeException {
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO CND");
		DPFTExpressionParser parser = new DPFTExpressionParser();
		if(parser.evaluate(condition)){
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE;
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_FALSE;
	}
	
	public int isTimeAfter(String time) throws DPFTRuntimeException {
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO CND");
		
		Date current = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT + "HH:mm");
		SimpleDateFormat sdf2 = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		try {
			Date target = sdf.parse(sdf2.format(current) + time);
			if(current.after(target))
				return GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE;
		} catch (ParseException e) {
			Object[] params = {"HH24:mm"};
			throw new DPFTAutomationException("SYSTEM", "AUTO0014E", params, e);
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_FALSE;
	}
	
	public int isProcessFinished(String group_id) throws DPFTRuntimeException {
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO CND");
		DPFTAutomationProcessSet pSet = (DPFTAutomationProcessSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("DPFT_AUTOMATION_PROCESS", "group_id='" + group_id + "'");
		if(pSet.isFinished()){
			pSet.close();
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE;
		}
		pSet.close();
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_FALSE;
	}
}

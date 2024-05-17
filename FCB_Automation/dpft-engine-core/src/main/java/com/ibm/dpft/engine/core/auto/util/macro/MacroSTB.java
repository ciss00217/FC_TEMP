package com.ibm.dpft.engine.core.auto.util.macro;

import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationConditionSet;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationMacro;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class MacroSTB extends DPFTAutomationMacro {

	public int ConditionMatch(String conditions) throws InterruptedException, DPFTRuntimeException {
		return _condition_matched(conditions, true);
	}
	
	public int ConditionMatch(String conditions, String notif_on_failed, DPFTMessage msg) throws InterruptedException, DPFTRuntimeException {
		int rc = _condition_matched(conditions, false);
		if(rc != GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL && notif_on_failed.equalsIgnoreCase("y")){
//			DPFTUtil.pushNotification(msg);
		}else if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL && !notif_on_failed.equalsIgnoreCase("y")){
//			DPFTUtil.pushNotification(msg);
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	private int _condition_matched(String conditions, boolean need_sleep) throws DPFTRuntimeException, InterruptedException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO STB");
		String[] condition_ids = conditions.split(GlobalConstants.FILE_DELIMETER_COMMA);
		DPFTAutomationConditionSet cndSet = getConditions(condition_ids);
		if(cndSet.isEmpty()){
			cndSet.close();
			Object[] params = {getProcess().getString("group_id")};
			throw new DPFTAutomationException("SYSTEM", "AUTO0015E", params);
		}
		if(cndSet.isAllConditionsMatched()){
			DPFTLogger.debug(this, "MACRO : All Condition Matched!!!");
			cndSet.close();
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		}
		DPFTLogger.debug(this, "MACRO : Still some conditions not matched. Keep Waiting...");
		cndSet.close();
		if(need_sleep)
			Thread.sleep(10000);
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_WAITING;
	}

	private DPFTAutomationConditionSet getConditions(String[] condition_ids) throws DPFTRuntimeException {
		return (DPFTAutomationConditionSet) getProcess().getDboSet("DPFT_AUTOMATION_COND", buildWhereStringByCndIDs(getProcess().getString("group_id"), condition_ids));
	}

	private String buildWhereStringByCndIDs(String gid, String[] condition_ids) {
		StringBuilder sb = new StringBuilder();
		sb.append("group_id='" + gid + "' and condition_id in (" + DPFTUtil.convertToString(condition_ids, true) + ")");
		return sb.toString();
	}

}

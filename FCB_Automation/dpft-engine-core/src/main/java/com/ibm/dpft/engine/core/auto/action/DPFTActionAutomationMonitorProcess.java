package com.ibm.dpft.engine.core.auto.action;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcessSet;
import com.ibm.dpft.engine.core.auto.taskplan.DPFTAutomationManager;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionAutomationMonitorProcess extends DPFTActionTableWatch {
	private DPFTAutomationManager ps_mgr = null;
	private final static long sleep_time = 60000;

	public DPFTActionAutomationMonitorProcess(DPFTAutomationManager mgr) {
		super();
		ps_mgr = mgr;
		
		/*default set watch intervals to 60 seconds*/
		this.setPostActionSleepTime(sleep_time);
	}

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		return "DPFT_AUTOMATION_PROCESS";
	}

	@Override
	public String getTableWatchCriteria() {
		return "process_status not in ('" + GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_FIN + "','" 
				+ GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_ERROR + "')";
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		DPFTAutomationProcessSet set = (DPFTAutomationProcessSet) this.getDataSet();
		if(!set.isEmpty()){
			this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_RUN);
			this.setPostActionSleepTime(sleep_time);
			set.close();
		}else{
			ps_mgr.stop();
		}
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		throw e;
	}

}

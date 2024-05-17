package com.ibm.dpft.engine.core.auto.action;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstanceSet;
import com.ibm.dpft.engine.core.auto.taskplan.DPFTAutomationManager;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionAutomationInitPSPool extends DPFTActionTableWatch {
	private DPFTAutomationManager ps_mgr = null;

	public DPFTActionAutomationInitPSPool(DPFTAutomationManager mgr) {
		super();
		ps_mgr = mgr;
	}
	
	@Override
	public void postAction() throws DPFTRuntimeException {
		if(ps_mgr.getProcessPool().isEmpty()){
			try {
				initProcessPool();
			} catch (DPFTConnectionException e) {
				throw new DPFTActionException(this, "SYSTEM", "DPFT0006E", e);
			}
			ps_mgr.start();
		}
	}

	private void initProcessPool() throws DPFTRuntimeException {
		DPFTAutomationPsInstanceSet psSet = (DPFTAutomationPsInstanceSet) this.getDataSet();
		if(!psSet.isEmpty())
			psSet.deleteAll();
		ps_mgr.setProcessPool(psSet.initRunnerInstancePool());
		psSet.save();
	}

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		return "DPFT_AUTOMATION_PS_INST";
	}

	@Override
	public String getTableWatchCriteria() {
		return null;
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		throw e;
	}

}

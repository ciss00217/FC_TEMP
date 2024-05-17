package com.ibm.dpft.engine.core.auto.action;

import java.lang.reflect.InvocationTargetException;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcessSet;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstance;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstanceSet;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationScriptTranslator;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTAutomationInstTableWatch extends DPFTActionTableWatch {
	private String run_id = null;

	public DPFTAutomationInstTableWatch(String run_id) {
		super();
		this.run_id = run_id;
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
		
		return "run_id='" + run_id + "'";
	}

	@Override
	public String getTriggerKeyCol() {
		
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		DPFTAutomationPsInstanceSet set = (DPFTAutomationPsInstanceSet) this.getDataSet();
		for(int i = 0; i < set.count(); i++){
			DPFTAutomationPsInstance inst = (DPFTAutomationPsInstance) set.getDbo(i);
			inst.setProcessSet(getProcessSet(inst.getString("group_id")));
			DPFTAutomationScriptTranslator trs = new DPFTAutomationScriptTranslator(inst.currentStep());
			String cmd = inst.currentStep().getString("argvs").split(GlobalConstants.FILE_DELIMETER_SHARP)[1];
			try {
				int rc;
				try {
					rc = trs.translateScript("Execute", "CMD", cmd).invoke();
					if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL){
						inst.setValue("run_id", GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL);
						set.save();
					}else if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_ERROR){
						inst.error();
						inst.setValue("run_id", GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL);
					}
				} catch (DPFTAutomationException e) {
					inst.error();
					inst.setValue("run_id", GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL);
					throw e;
				}
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				Object[] params = {cmd};
				throw new DPFTActionException(this, "SYSTEM", "AUTO0007E", params, e);
			}
		}

	}

	private DPFTAutomationProcessSet getProcessSet(String group_id) throws DPFTRuntimeException {
		DPFTAutomationProcessSet psSet = (DPFTAutomationProcessSet) getAutoProcessSet(group_id);
		if(psSet.isEmpty()){
			psSet.close();
			Object[] params = {group_id};
			throw new DPFTAutomationException("SYSTEM", "AUTO0012E", params);
		}
		return psSet;
	}
	
	

	private DPFTAutomationProcessSet getAutoProcessSet(String group_id) throws DPFTRuntimeException {
		try {
			return (DPFTAutomationProcessSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("DPFT_AUTOMATION_PROCESS", "group_id='" + group_id + "'");
		} catch (DPFTConnectionException e) {
			throw new DPFTActionException(this, "SYSTEM", "DPFT0003E", e);
		}
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		
		
	}

}

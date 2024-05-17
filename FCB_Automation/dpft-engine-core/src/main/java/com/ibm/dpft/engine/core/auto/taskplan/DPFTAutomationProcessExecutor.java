package com.ibm.dpft.engine.core.auto.taskplan;

import com.ibm.dpft.engine.core.auto.action.DPFTActionAutomationExecStep;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcessSet;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstance;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstanceSet;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTAutomationProcessExecutor extends DPFTBaseTaskPlan {
	private DPFTAutomationPsInstanceSet instSet = null;
	private DPFTAutomationProcessSet psSet = null;
	private DPFTConnector connector = null;
	private String group_id = null;
	
	
	public DPFTAutomationProcessExecutor(String id) {
		super(id);
	}

	public DPFTAutomationProcessExecutor(DPFTBaseTaskPlan tp) {
		super(tp);
	}
	
	@Override
	public void doTask() throws InterruptedException, DPFTRuntimeException{
		try{
			initConnector();
			initInstanceDboSet();
			initProcessDboSet();
			checkin();
		}catch(Exception e){
			if(e instanceof DPFTRuntimeException)
				((DPFTRuntimeException) e).handleException();
			DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
			ex.handleException();
			throw ex;
		}
		try{
			super.doTask();
		}catch(Exception e){
			closeSession();
			throw e;
		}
		
	}

	private void closeSession() throws DPFTRuntimeException {
		if(instSet != null)
			instSet.close();
		instSet = null;
		psSet = null;
	}

	private void checkin() throws DPFTRuntimeException {
		((DPFTAutomationPsInstance)instSet.getDbo(0)).runnable();
		SaveAndRefresh();
//		DPFTUtil.pushNotification(new DPFTMessage("SYSTEM", "DPFT0038I"));
	}

	private void SaveAndRefresh() throws DPFTRuntimeException {
		instSet.save();
		psSet.save();
		if(instSet.isEmpty()){
			this.changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
			Object[] params = {group_id};
			throw new DPFTAutomationException("SYSTEM", "AUTO0010E", params);
		}
		if(psSet.isEmpty()){
			this.changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
			Object[] params = {group_id};
			throw new DPFTAutomationException("SYSTEM", "AUTO0012E", params);
		}
		((DPFTAutomationPsInstance)instSet.getDbo(0)).setProcessSet(psSet);
	}

	private void initConnector() throws DPFTRuntimeException {
		if(connector == null)
			connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig());
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionAutomationExecStep(this));
	}

	@Override
	public boolean isRecurring() {
		return false;
	}

	public void setProcessInstanceDboSet(DPFTAutomationPsInstanceSet dboset) {
		instSet = dboset;
	}
	
	public DPFTAutomationPsInstanceSet getProcessInstanceDboSet() {
		return instSet;
	}
	
	private void initProcessDboSet() throws DPFTRuntimeException {
		psSet = (DPFTAutomationProcessSet) connector.getDboSet("DPFT_AUTOMATION_PROCESS", "group_id='" + group_id + "'");
		
		if(psSet.isEmpty()){
			this.changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
			Object[] params = {group_id};
			throw new DPFTAutomationException("SYSTEM", "AUTO0012E", params);
		}
		((DPFTAutomationPsInstance)instSet.getDbo(0)).setProcessSet(psSet);
	}
	
	private void initInstanceDboSet() throws DPFTRuntimeException {	
		instSet = (DPFTAutomationPsInstanceSet) connector.getDboSet("DPFT_AUTOMATION_PS_INST", "group_id='" + group_id + "'");
		
		if(instSet.isEmpty()){
			this.changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
			Object[] params = {group_id};
			throw new DPFTAutomationException("SYSTEM", "AUTO0010E", params);
		}
	}

	public DPFTAutomationProcessSet getProcessDboSet() {
		return psSet;
	}

	public void executeCurrentStep() throws DPFTRuntimeException {
		DPFTAutomationPsInstance inst = ((DPFTAutomationPsInstance)instSet.getDbo(0));
		if(inst.isCurrentStepRunnable()){
			int rc = initiateRunningState(inst).executeCurrentStep();
			readRC(rc);
		}else if(inst.isCurrentStepRunning()){
			int rc = inst.executeCurrentStep();
			readRC(rc);
		}else if(inst.isCurrentStepError()){
			goNextStep(inst);
		}else if(inst.isCurrentStepFinished()){
			goNextStep(inst);
		}
	}

	private void goNextStep(DPFTAutomationPsInstance inst) throws DPFTRuntimeException {
		if(inst.hasNextStep()){
			inst.move2NextStep();
			SaveAndRefresh();
		}else{
			//Whole group process finished, Exiting
			inst.exit();
			SaveAndRefresh();
			this.changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
			closeSession();
		}
	}

	public void readRC(int rc) throws DPFTRuntimeException {
		DPFTAutomationPsInstance inst = ((DPFTAutomationPsInstance)instSet.getDbo(0));
		if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL){
			finishing(inst);
			Object[] params = {group_id, inst.getString("step_id"), inst.currentStep().getString("ps_desc")};
//			DPFTUtil.pushNotification(new DPFTMessage("SYSTEM", "DPFT0040I", params));
		}else if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_ERROR){
			error(inst);
			Object[] params = {group_id, inst.getString("step_id"), inst.currentStep().getString("ps_desc")};
//			DPFTUtil.pushNotification(new DPFTMessage("SYSTEM", "DPFT0041I", params));
		}
	}

	private void error(DPFTAutomationPsInstance inst) throws DPFTRuntimeException {
		inst.error();
		SaveAndRefresh();
	}

	private void finishing(DPFTAutomationPsInstance inst) throws DPFTRuntimeException {
		inst.finishing();
		SaveAndRefresh();
	}

	private DPFTAutomationPsInstance initiateRunningState(DPFTAutomationPsInstance inst) throws DPFTRuntimeException {
		inst.running();
		SaveAndRefresh();
		Object[] params = {group_id, inst.getString("step_id"), inst.currentStep().getString("ps_desc")};
//		DPFTUtil.pushNotification(new DPFTMessage("SYSTEM", "DPFT0039I", params));
		return ((DPFTAutomationPsInstance)instSet.getDbo(0));
	}

	public boolean isCurrentStepFinished() {
		
		return false;
	}

	public void setGroupID(String gid) {
		group_id = gid;
	}

}

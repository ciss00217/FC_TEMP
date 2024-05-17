package com.ibm.dpft.engine.core.auto.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTAutomationPsInstance extends DPFTDbo {

	private DPFTAutomationProcessSet psSet = null;

	public DPFTAutomationPsInstance(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);
		
	}

	public DPFTAutomationProcess currentStep() throws DPFTRuntimeException {
		return getProcessStep(this.getString("step_id"));
	}
	
	private DPFTAutomationProcess getProcessStep(String step_id) throws DPFTRuntimeException{
		return (DPFTAutomationProcess) psSet.getProcess(step_id);
	}
	
	private boolean hasProcessStep(String step_id) throws DPFTRuntimeException{
		return (psSet.getProcess(step_id) != null);
	}

	public boolean isCurrentStepFinished() throws DPFTRuntimeException {
		return currentStep().isFinished();
	}

	public boolean hasNextStep() throws DPFTRuntimeException {
		int next_sid = Integer.valueOf(this.getString("step_id")) + 1;
		return hasProcessStep(String.valueOf(next_sid));
	}

	public void move2NextStep() throws DPFTRuntimeException {
		int next_sid = Integer.valueOf(this.getString("step_id")) + 1;
		this.setValue("step_id", String.valueOf(next_sid));
		this.setValue("run_time", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("run_log", "MSG: Mark Next Step Runnable");
		
		DPFTAutomationProcess process = getProcessStep(String.valueOf(next_sid));
		process.setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_RUNNABLE);
	}

	public void setProcessSet(DPFTAutomationProcessSet processDboSet) {
		psSet  = processDboSet;
	}

	public void finished() {
		
		
	}

	public int executeCurrentStep() throws DPFTRuntimeException {
		return currentStep().exec();
	}

	public void runnable() throws DPFTRuntimeException {
		DPFTAutomationProcess process = currentStep();
		process.setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_RUNNABLE);
		this.setValue("run_time", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("run_log", "MSG: Mark Current Step Runnable");
	}
	
	public void running() throws DPFTRuntimeException {
		DPFTAutomationProcess process = currentStep();
		process.setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_RUNNING);
		this.setValue("run_time", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("run_log", "MSG: Start Executing...");
	}
	
	public void finishing() throws DPFTRuntimeException {
		DPFTAutomationProcess process = currentStep();
		process.setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_FIN);
		this.setValue("run_time", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("run_log", "MSG: Process Return Normal, step finish...");
	}
	
	public void error() throws DPFTRuntimeException {
		DPFTAutomationProcess process = currentStep();
		process.setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_ERROR);
		this.setValue("run_time", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("run_log", "MSG: Process Return Error...");
	}
	
	public void exit() {
		this.setValue("run_time", DPFTUtil.getCurrentTimeStampAsString());
		this.setValue("run_log", "MSG: " + this.getString("group_id") + " exiting execution runner...");
	}

	public boolean isCurrentStepRunnable() throws DPFTRuntimeException {
		return currentStep().isRunnable();
	}

	public boolean isCurrentStepRunning() throws DPFTRuntimeException {
		
		return currentStep().isRunning();
	}

	public boolean isCurrentStepError() throws DPFTRuntimeException {
		
		return currentStep().isError();
	}

	public void close() throws DPFTRuntimeException {
		if(psSet != null)
			psSet.close();
	}
}

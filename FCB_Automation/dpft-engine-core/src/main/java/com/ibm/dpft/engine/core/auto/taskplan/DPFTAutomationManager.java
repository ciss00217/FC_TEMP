package com.ibm.dpft.engine.core.auto.taskplan;

import java.util.ArrayList;

import com.ibm.dpft.engine.core.auto.DPFTAutomationTaskRunner;
import com.ibm.dpft.engine.core.auto.action.DPFTActionAutomationInitPSPool;
import com.ibm.dpft.engine.core.auto.action.DPFTActionAutomationMonitorProcess;
import com.ibm.dpft.engine.core.auto.util.macro.MacroVar;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTAutomationManager extends DPFTBaseTaskPlan {
	private ArrayList<DPFTAutomationTaskRunner> ps_pool = new ArrayList<DPFTAutomationTaskRunner>();
	
	public DPFTAutomationManager(String id) {
		super(id);
	}


	public DPFTAutomationManager(DPFTBaseTaskPlan tp) {
		super(tp);
	}

	@Override
	public void setActionsForPlan() {
		this.getActionList().add(new DPFTActionAutomationInitPSPool(this));
		this.getActionList().add(new DPFTActionAutomationMonitorProcess(this));
	}

	@Override
	public boolean isRecurring() {
		return false;
	}


	public ArrayList<DPFTAutomationTaskRunner> getProcessPool() {
		return ps_pool;
	}


	public void setProcessPool(ArrayList<DPFTAutomationTaskRunner> arrayList) {
		this.ps_pool = arrayList;
	}


	public void start() {
		MacroVar.clearCache();
		for(DPFTAutomationTaskRunner runner: ps_pool){
			runner.start();
		}
	}


	public void stop() {
		for(DPFTAutomationTaskRunner runner: ps_pool){
			if(runner.isAlive()){
				//Task still running, interrupt...
				DPFTLogger.info(this, "Automation Process : " + runner.getId() + " is still running. Force stopping...");
				runner.interrupt();
			}
		}
		ps_pool.clear();
	}

}

package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDbo;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionObndPeriodicTrigger extends DPFTActionTaskTrigger {

	@Override
	public DPFTTriggerMapDefDboSet getTriggerMap() {
		
		return null;
	}
	
	@Override
	public void action() throws DPFTRuntimeException {
		getRunnerlist().clear();
		DPFTTriggerMapDefDboSet tmapSet = (DPFTTriggerMapDefDboSet) this.getPreviousAction().getResultSet();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < tmapSet.count(); i++){
			DPFTTriggerMapDefDbo dbo = (DPFTTriggerMapDefDbo) tmapSet.getDbo(i);
			String tpid = dbo.getString("planid");
			if(tpid != null && !tpid.isEmpty() && dbo.canInvoke()){
				DPFTTaskRunner thread = (DPFTTaskRunner) DPFTTaskRunner.currentThread();
				DPFTTaskRunner new_runner = thread.getMgr().createNewRunnerByTaskPlanID(tpid);
				new_runner.setInitialData(dbo);
				getRunnerlist().add(new_runner);
				sb.append("'").append(dbo.getPrimaryKeyValue()).append("',");
			}
		}
		if(!getRunnerlist().isEmpty()){
			//prevent next iteration to trigger unfinished task
			DPFTTriggerMapDefDboSet trSet = (DPFTTriggerMapDefDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_OBND_TP_DEF", "pid in (" + sb.substring(0, sb.length()-1) + ")");
			for(int i = 0; i < trSet.count(); i++){
				trSet.getDbo(i).setValue("active", "0");
			}
			trSet.save();
			trSet.close();
		}
		
		logRunner();
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		
	}
}

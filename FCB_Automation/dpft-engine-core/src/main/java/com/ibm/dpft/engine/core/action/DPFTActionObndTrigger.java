package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionObndTrigger extends DPFTActionTaskTrigger {
	

	@Override
	public DPFTTriggerMapDefDboSet getTriggerMap() throws DPFTRuntimeException {
		/*get channel outbound Task Plan Mapping*/
		try {
			return DPFTUtil.getObndTriggerMap();
		} catch (DPFTConnectionException e) {
			throw new DPFTActionException(this, "SYSTEM", "DPFT0004E", e);
		}
	}

	@Override
	public void action() throws DPFTRuntimeException {
		
		DPFTTriggerMapDefDboSet tmapSet = getTriggerMap();
		getRunnerlist().clear();
		if((this.getPreviousAction() instanceof DPFTActionTableWatch)){
			DPFTOutboundControlDboSet dataset = (DPFTOutboundControlDboSet) ((DPFTActionTableWatch)this.getPreviousAction()).getDataSet();
			boolean[] exec_tpid_indx = dataset.getExecTaskPlanIndice(tmapSet,((DPFTActionTableWatch)this.getPreviousAction()).getTriggerKeyCol());
			for(int i = 0; i < dataset.count(); i++){
				DPFTDbo dbo = dataset.getDbo(i);
				String tpid = tmapSet.getTpTriggerMap().get(dbo.getString(((DPFTActionTableWatch)this.getPreviousAction()).getTriggerKeyCol()));
				if(tpid != null && !tpid.isEmpty() && exec_tpid_indx[i]){
					DPFTTaskRunner thread = (DPFTTaskRunner) DPFTTaskRunner.currentThread();
					DPFTTaskRunner new_runner = thread.getMgr().createNewRunnerByTaskPlanID(tpid);
					new_runner.setInitialData(dbo);
					getRunnerlist().add(new_runner);
				}
			}
			logRunner();
			this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
			tmapSet.close();
		}
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		
	}
}

package com.ibm.dpft.engine.core;

import java.util.ArrayList;
import java.util.List;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.TaskPlanDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.taskplan.DPFTBaseTaskPlan;
import com.ibm.dpft.engine.core.taskplan.DPFTTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTScheduler {
	private List<DPFTTaskPlan> tplist = new ArrayList<DPFTTaskPlan>();
	private List<DPFTTaskPlan> system_tplist = new ArrayList<DPFTTaskPlan>();
	private List<DPFTTaskPlan> passive_tplist = new ArrayList<DPFTTaskPlan>();
	//private DPFTConnector connector = null;
	private DPFTConfig sys_cfg;

	public DPFTScheduler(DPFTConfig sys_cfg) {
		
		this.sys_cfg  = sys_cfg;
	}

	public void registerTaskPlan(DPFTTaskPlan tp, boolean isDaemonPlan) {
		
		if(isDaemonPlan){
			DPFTLogger.debug(this, "Registered Startup Engine Task Plan class : " + tp.getClass().getName());
			system_tplist.add(tp);
		}else{
			DPFTLogger.debug(this, "Registered Task Plan class : " + tp.getClass().getName());
			tplist.add(tp);
		}
	}

	public void registerTaskPlanFromDB() throws DPFTRuntimeException {
		
		DPFTLogger.info(this, "Start loading task plan from DB...");
		TaskPlanDefDboSet planSet = (TaskPlanDefDboSet) DPFTConnectionFactory.initDPFTConnector(sys_cfg).getDboSet("DPFT_TASKPLAN_DEF", "active=?");
		planSet.setBoolean(1, true);
		planSet.load();
		planSet.close();
		try {
			List<DPFTTaskPlan> taskplans = planSet.convert2TaskPlanList();
			for(DPFTTaskPlan tp : taskplans){
				if(tp.getId().indexOf(GlobalConstants.DPFT_TP_KEY_PSV) != -1)
					registerPassiveTaskPlan(tp);
				else
					registerTaskPlan(tp, false);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DPFTRuntimeException("SYSTEM", "DPFT0036E", e);
		} 
		DPFTLogger.info(this, "DB Task Plan definitions sucessfully registered...");
	}

	private void registerPassiveTaskPlan(DPFTTaskPlan tp) {
		DPFTLogger.debug(this, "Registered Passive Task Plan class : " + tp.getClass().getName());
		passive_tplist.add(tp);
	}

	public List<DPFTTaskPlan> getSystemTaskPlan() {
		
		return system_tplist;
	}
	
	public DPFTTaskPlan getRegisteredTaskPlanById(String tpid) {
		
		for(DPFTTaskPlan tp: tplist){
			if(((DPFTBaseTaskPlan)tp).getId().equalsIgnoreCase(tpid))
				return tp;
		}
		return null;
	}

	public List<DPFTTaskPlan> getPassiveTaskPlan() {
		
		return passive_tplist;
	}

}

package com.ibm.dpft.engine.core.action;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public abstract class DPFTActionTaskTrigger extends DPFTAction implements DPFTTaskTriggerInterface {

	private ArrayList<DPFTTaskRunner> runnerlist = new ArrayList<DPFTTaskRunner>();

	public DPFTActionTaskTrigger() {
		super();
	}

	@Override
	public void action() throws DPFTRuntimeException {

		DPFTTriggerMapDefDboSet tmapSet = getTriggerMap();
		runnerlist.clear();
		if ((this.getPreviousAction() instanceof DPFTActionTableWatch)) {
			DPFTDboSet dataset = ((DPFTActionTableWatch) this.getPreviousAction()).getDataSet();
//			DPFTLogger.info(this, "dataset.count():" + dataset.count());
			for (int i = 0; i < dataset.count(); i++) {
				DPFTDbo dbo = dataset.getDbo(i);
				String tpid = tmapSet.getTpTriggerMap()
						.get(dbo.getString(((DPFTActionTableWatch) this.getPreviousAction()).getTriggerKeyCol()));
//				DPFTLogger.info(this, "tpid:" + tpid);
				if (tpid != null && !tpid.isEmpty()) {
					DPFTTaskRunner thread = (DPFTTaskRunner) DPFTTaskRunner.currentThread();
					DPFTTaskRunner new_runner = thread.getMgr().createNewRunnerByTaskPlanID(tpid);
					new_runner.setInitialData(dbo);
					runnerlist.add(new_runner);
				}
			}
			this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
			tmapSet.close();
		}
	}

	@Override
	public void clean() throws DPFTRuntimeException {
		if (this.getResultSet() != null)
			this.getResultSet().clear();
	}

	@Override
	public void finish() {

		((DPFTTaskRunner) DPFTTaskRunner.currentThread()).getMgr().start(runnerlist);
	}

	public ArrayList<DPFTTaskRunner> getRunnerlist() {
		return runnerlist;
	}

	protected void logRunner() {

		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (DPFTTaskRunner tr : getRunnerlist()) {
			if (map.containsKey(tr.getTaskPlan().getClass().getName())) {
				int cnt = map.get(tr.getTaskPlan().getClass().getName());
				cnt++;
				map.put(tr.getTaskPlan().getClass().getName(), cnt);
			} else {
				map.put(tr.getTaskPlan().getClass().getName(), 1);
			}
		}

		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		for (String key : map.keySet()) {
			sb.append("Ready to Execute Task Plan:").append(key).append(" Runner Instance = " + map.get(key) + "\n");
		}
	}

}

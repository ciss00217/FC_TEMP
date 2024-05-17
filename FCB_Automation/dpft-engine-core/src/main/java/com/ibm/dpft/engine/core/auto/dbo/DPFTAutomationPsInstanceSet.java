package com.ibm.dpft.engine.core.auto.dbo;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.auto.DPFTAutomationTaskRunner;
import com.ibm.dpft.engine.core.auto.taskplan.DPFTAutomationProcessExecutor;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationPsInstanceSet extends DPFTDboSet {

	public DPFTAutomationPsInstanceSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTAutomationPsInstance(dboname, d, this);
	}

	public ArrayList<DPFTAutomationTaskRunner> initRunnerInstancePool() throws DPFTRuntimeException {
		DPFTAutomationProcessSet psSet = (DPFTAutomationProcessSet) this.getDBConnector()
				.getDboSet("DPFT_AUTOMATION_PROCESS");
		String[] gids = psSet.getGroupIDList();
		psSet.close();
		ArrayList<DPFTAutomationTaskRunner> pool = new ArrayList<DPFTAutomationTaskRunner>();
		for (String gid : gids) {
			DPFTAutomationPsInstance new_instance = (DPFTAutomationPsInstance) this.add();
			new_instance.setValue("group_id", gid);
			new_instance.setValue("step_id", "0");
			new_instance.setValue("run_id", GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL);
			new_instance.setValue("run_log", "MSG: Initializing...");

			DPFTAutomationTaskRunner new_runner = new DPFTAutomationTaskRunner(
					new DPFTAutomationProcessExecutor(GlobalConstants.DPFT_AUTOMATION_EXECUTOR_TPID));
			new_runner.setGroupID(gid);
			pool.add(new_runner);
		}
		return pool;
	}

	@Override
	public void save() throws DPFTRuntimeException {
		DPFTAutomationInstLogSet hisSet = (DPFTAutomationInstLogSet)
		this.getDBConnector().getDboSet("DPFT_AUTOMATION_INST_LOG",
		       "rownum<=1");
		// DPFTAutomationInstLogSet hisSet = (DPFTAutomationInstLogSet) this.getDBConnector().getDboSet("TOP 1 *",
		// 		"DPFT_AUTOMATION_INST_LOG", "");
		for (int i = 0; i < count(); i++) {
			if (this.getDbo(i).tobeAdded() || this.getDbo(i).tobeSaved()) {
				hisSet.addHistory(this.getDbo(i));
			}
		}
		super.save();
		hisSet.save();
		hisSet.close();
	}

	@Override
	public void close() throws DPFTRuntimeException {
		super.close();
		for (int i = 0; i < count(); i++) {
			DPFTAutomationPsInstance inst = (DPFTAutomationPsInstance) this.getDbo(i);
			inst.close();
		}
	}

}

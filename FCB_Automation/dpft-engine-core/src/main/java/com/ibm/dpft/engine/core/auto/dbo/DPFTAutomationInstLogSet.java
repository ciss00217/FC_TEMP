package com.ibm.dpft.engine.core.auto.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationInstLogSet extends DPFTDboSet {
	
	public DPFTAutomationInstLogSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTAutomationInstLog(dboname, d, this);
	}

	public void addHistory(DPFTDbo dbo) throws DPFTRuntimeException {
		
		DPFTAutomationInstLog log = (DPFTAutomationInstLog) this.add();
		log.setValue("group_id", dbo.getString("group_id"));
		log.setValue("step_id" , dbo.getString("step_id"));
		log.setValue("run_id"  , dbo.getString("run_id"));
		log.setValue("run_time", dbo.getString("run_time"));
		log.setValue("run_log" , dbo.getString("run_log"));
	}

}

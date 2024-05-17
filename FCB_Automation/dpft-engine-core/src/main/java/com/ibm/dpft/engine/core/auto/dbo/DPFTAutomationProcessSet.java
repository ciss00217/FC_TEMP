package com.ibm.dpft.engine.core.auto.dbo;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationProcessSet extends DPFTDboSet {

	public DPFTAutomationProcessSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTAutomationProcess(dboname, d, this);
	}

	public void addNewProcess(String group_id, String step_id, String desc, String ps_def) throws DPFTRuntimeException {
		
		String[] args = ps_def.split(GlobalConstants.FILE_DELIMETER_SEMICOLON);
		String[] acts = args[0].split(GlobalConstants.FILE_DELIMETER_DOT);
		String action = acts[0];
		String macro = acts[1];
		String argvs = (args.length > 1)?args[1]:null;
		DPFTAutomationProcess new_ps = (DPFTAutomationProcess) this.add();
		new_ps.setValue("group_id", group_id);
		new_ps.setValue("step_id", step_id);
		new_ps.setValue("ps_desc", desc);
		new_ps.setValue("action", action);
		new_ps.setValue("macro", macro);
		new_ps.setValue("argvs", argvs);
		new_ps.setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_INIT);
	}

	public String[] getGroupIDList() throws DPFTRuntimeException {
		ArrayList<String> list = new ArrayList<String>();
		for(int i = 0; i < this.count(); i++){
			if(list.contains(this.getDbo(i).getString("group_id")))
				continue;
			list.add(this.getDbo(i).getString("group_id"));
		}
		return list.toArray(new String[list.size()]);
	}

	public DPFTAutomationProcess getProcess(String step_id) throws DPFTRuntimeException {
		for(int i = 0; i < count(); i++){
			if(this.getDbo(i).getString("step_id").equals(step_id))
				return (DPFTAutomationProcess) this.getDbo(i);
		}
		return null;
	}

	public void markAsRunnable() throws DPFTRuntimeException {
		for(int i = 0; i < count(); i++){
			this.getDbo(i).setValue("process_status", GlobalConstants.DPFT_AUTOMATION_PROCESS_STATUS_RUNNABLE);
		}
		this.save();
	}

	public boolean isFinished() throws DPFTRuntimeException {
		for(int i = 0; i < this.count(); i++){
			DPFTAutomationProcess p = (DPFTAutomationProcess) this.getDbo(i);
			if(!p.isFinished() && !p.isError())
				return false;
		}
		this.close();
		return true;
	}

}

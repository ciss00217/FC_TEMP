package com.ibm.dpft.engine.core.auto.util.macro;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcessSet;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstance;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationPsInstanceSet;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationMacro;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTBashRunner;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class MacroCMD extends DPFTAutomationMacro {
	public int Invoke(String cmd) throws DPFTRuntimeException {
		return Execute(GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL, cmd, "N");
	}

	public int Execute(String cmd) throws DPFTRuntimeException {
		return Execute(GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL, cmd, "Y");
	}

	public int Execute(String run_id, String cmd) throws DPFTRuntimeException {
		return Execute(run_id, cmd, "Y");
	}

	public int Execute(String run_id, String cmd, String wait_rtn_flg) throws DPFTRuntimeException {
		if (run_id.equals(GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL)) {
			return _execute(cmd, wait_rtn_flg.equalsIgnoreCase("y"));
		} else {
			// set current running instance run_id
			String group_id = getProcess().getString("group_id");
			DPFTAutomationPsInstanceSet set = (DPFTAutomationPsInstanceSet) DPFTConnectionFactory
					.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_AUTOMATION_PS_INST", "group_id='" + group_id + "'");
			if (set.isEmpty()) {
				set.close();
				Object[] params = { group_id };
				throw new DPFTAutomationException("SYSTEM", "AUTO0010E", params);
			}
			set.getDbo(0).setValue("run_id", run_id);
			set.save();
			return isRemoteProcessFinished(set);
		}
	}

	private int isRemoteProcessFinished(DPFTAutomationPsInstanceSet set) throws DPFTRuntimeException {
		boolean isError = false;
		String error_run_id = null;
		while (true) {
			DPFTAutomationPsInstance inst = (DPFTAutomationPsInstance) set.getDbo(0);
			inst.setProcessSet(getProcessSet(inst.getString("group_id")));
			if (inst.getString("run_id").equals(GlobalConstants.DPFT_AUTOMATION_PS_RUN_ID_LOCAL)) {
				if (inst.isCurrentStepError()) {
					isError = true;
					error_run_id = inst.getString("run_id");
				}
				break;
			}
			try {
				DPFTLogger.debug(this,
						"Still Waiting for remote process on " + set.getDbo(0).getString("run_id") + " to finish...");
				Thread.sleep(1000);
				set.refresh();
			} catch (InterruptedException e) {
				set.close();
				Object[] params = { inst.getString("run_id") };
				throw new DPFTAutomationException("SYSTEM", "AUTO0011E", params, e);
			}
		}
		set.close();
		if (isError) {
			Object[] params = { error_run_id };
			throw new DPFTAutomationException("SYSTEM", "AUTO0011E", params);
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE;
	}

	private DPFTAutomationProcessSet getProcessSet(String group_id) throws DPFTRuntimeException {
		DPFTAutomationProcessSet psSet = (DPFTAutomationProcessSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("DPFT_AUTOMATION_PROCESS", "group_id='" + group_id + "'");
		if (psSet.isEmpty()) {
			psSet.close();
			Object[] params = { group_id };
			throw new DPFTAutomationException("SYSTEM", "AUTO0012E", params);
		}
		psSet.close();
		return psSet;
	}

	private int _execute(String cmd, boolean wait_rtn_code) throws DPFTRuntimeException {
		String logfile = DPFTEngine.getSystemProperties(GlobalConstants.DPFT_SYS_PROP_AUTO_MACRO_CMD_LOGPATH);
		DPFTBashRunner br = new DPFTBashRunner();
		br.setBashCmd(cmd);
		br.setLogFile(logfile + "_" + DPFTUtil.getCurrentTimeStampAsString());
		try {
			int errorlevel = br.execute(wait_rtn_code);
			if (errorlevel != GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL) {
				Object[] params = { cmd, String.valueOf(errorlevel) };
				throw new DPFTAutomationException("SYSTEM", "AUTO0013E", params);
			}
			return errorlevel;
		} catch (Exception e) {
			Object[] params = { cmd };
			throw new DPFTAutomationException("SYSTEM", "AUTO0007E", params, e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		String CMD="D:\\IBM\\DPFT_Runtime_dev\\resources\\test\\test.bat";
		String logfile="D:\\IBM\\DPFT_Runtime_dev\\resources\\test\\asd.log";
		
				DPFTBashRunner br = new DPFTBashRunner();
		br.setBashCmd(CMD);
		br.setLogFile(logfile + "_" + DPFTUtil.getCurrentTimeStampAsString());
		try {
			int errorlevel = br.execute(true);
			if (errorlevel != GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL) {
				Object[] params = { CMD, String.valueOf(errorlevel) };
				throw new DPFTAutomationException("SYSTEM", "AUTO0013E", params);
			}
			
		} catch (Exception e) {
			
			throw  e;
		}
		
	}

}

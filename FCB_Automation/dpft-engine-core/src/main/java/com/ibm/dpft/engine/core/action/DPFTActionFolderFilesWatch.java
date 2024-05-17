package com.ibm.dpft.engine.core.action;

import java.io.File;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public abstract class DPFTActionFolderFilesWatch extends DPFTAction {
	public abstract String getFolderDir();
	public abstract void postAction(File[] files) throws DPFTRuntimeException;

	@Override
	public void action() throws DPFTRuntimeException {
		String dir = getFolderDir();
		File folder = new File(dir);
		if(!folder.isDirectory() || !folder.exists()){
			Object[] params = {dir};
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0047E", params);
		}
		File[] files = folder.listFiles();
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
		postAction(files);
	}

}

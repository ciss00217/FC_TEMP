package com.ibm.dpft.engine.core.action;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public abstract class DPFTActionReadScript extends DPFTAction {
	protected Properties prop = null;
	
	abstract public String getScriptName();
	abstract public void loadScript() throws DPFTRuntimeException;

	@Override
	public void action() throws DPFTRuntimeException {
		//read script from resources
		prop = getResources(getScriptName());
		loadScript();
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}

	private Properties getResources(String scriptName) throws DPFTRuntimeException {
		InputStream input = null;
		if(scriptName == null)
			throw new DPFTInvalidSystemSettingException("SYSTEM", "AUTO0002E");
		try {
			input = new FileInputStream(scriptName);
			Reader reader = new InputStreamReader(input, "UTF-8");
			Properties p = new Properties();
			p.load(reader);
			return p;
		} catch (Exception e) {
			throw new DPFTAutomationException("SYSTEM", "AUTO0001E", e);
		} finally {
			if(input != null){
				try {
					input.close();
				} catch (IOException e) {
					throw new DPFTAutomationException("SYSTEM", "AUTO0001E", e);
				}
			}
		}
	}
}

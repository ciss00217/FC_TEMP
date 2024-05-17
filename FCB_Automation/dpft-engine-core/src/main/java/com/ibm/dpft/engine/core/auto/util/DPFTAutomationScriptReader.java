package com.ibm.dpft.engine.core.auto.util;

import java.util.HashMap;
import java.util.Properties;

import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationConditionSet;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationProcessSet;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationVariablesSet;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTAutomationScriptReader {
	private HashMap<String, DPFTDboSet> dboHM = new HashMap<String, DPFTDboSet>();
	private Properties prop = null;
	
	public void load(Properties prop) throws DPFTRuntimeException {
		this.prop = prop;
		initAutoDboSet(prop);
		for(Object key: prop.keySet()){
			String k = (String) key;
			String[] ks = k.split(GlobalConstants.FILE_DELIMETER_DOT);
			DPFTDboSet set = dboHM.get(ks[0]);
			if(set instanceof DPFTAutomationProcessSet){
				String desc = null;
				if(ks.length > 3)
					desc = ks[3];
				
				((DPFTAutomationProcessSet)set).addNewProcess(ks[1], ks[2], desc, prop.getProperty(k));
			}else if(set instanceof DPFTAutomationVariablesSet){
				((DPFTAutomationVariablesSet)set).addNewVariable(ks[1], ks[2], prop.getProperty(k));
			}else if(set instanceof DPFTAutomationConditionSet){
				((DPFTAutomationConditionSet)set).addNewCondition(ks[1], ks[2], prop.getProperty(k));
			}
		}
		saveAutoDboSet();
	}

	private void saveAutoDboSet() throws DPFTRuntimeException {
		for(String key: dboHM.keySet()){
			dboHM.get(key).save();
			dboHM.get(key).close();
		}
	}

	private void initAutoDboSet(Properties prop) throws DPFTRuntimeException {
		dboHM.clear();
		DPFTConnector conn = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig());
		for(Object key: prop.keySet()){
			String k = (String) key;
			if(k.indexOf(GlobalConstants.DPFT_AUTOMATION_TBL_STR) != -1){
				//Automation Table definition
				String keyword = k.split(GlobalConstants.FILE_DELIMETER_DOT)[2];
				String tblname = prop.getProperty(k);
				DPFTDboSet set = conn.getDboSet(tblname);
				if(set.count() > 0){
					DPFTLogger.info(this, keyword + " automation table = " + tblname + " clear table records...");
					set.deleteAll();
				}
				DPFTLogger.info(this, keyword + " automation table = " + tblname + " successfully initialized...");
				dboHM.put(keyword, set);
			}
		}
	}

	public String getProcessManagerID() {
		return prop.getProperty(GlobalConstants.DPFT_AUTOMATION_PS_MGR_STR);
	}

}

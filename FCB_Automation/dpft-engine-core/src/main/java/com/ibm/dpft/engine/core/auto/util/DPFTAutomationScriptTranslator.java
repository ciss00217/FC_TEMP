package com.ibm.dpft.engine.core.auto.util;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationMacroDefSet;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationVariables;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationVariablesSet;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTAutomationScriptTranslator {
	private DPFTDbo ps = null;

	public DPFTAutomationScriptTranslator(DPFTDbo dbo) {
		ps = dbo;
	}

	public DPFTAutomationMacro translateScript(String action, String macro, String argvs) throws DPFTRuntimeException {
		String[] args = (argvs == null)?null:argvs.split(GlobalConstants.FILE_DELIMETER_SHARP);		
		return loadMACRO(action, macro, buildArguments(args));
	}

	private DPFTAutomationMacro loadMACRO(String action, String macro, Object[] args) throws DPFTRuntimeException {
		DPFTAutomationMacroDefSet mSet = (DPFTAutomationMacroDefSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("DPFT_AUTOMATION_MACRO", "macro_id='" + macro + "' and active=1");
		if(mSet.isEmpty()){
			mSet.close();
			Object[] params = {macro};
			throw new DPFTAutomationException("SYSTEM", "AUTO0021E", params);
		}
		mSet.close();
		String classname = mSet.getDbo(0).getString("classname");
		return generateAutomationMacro(classname, action, args);
	}

	private DPFTAutomationMacro generateAutomationMacro(String classname, String action, Object[] args) throws DPFTRuntimeException {
		DPFTAutomationMacro macro = null;
		try {
			macro = DPFTMacroGenerator.initMacroObject(classname);
		} catch (Exception e) {
			throw new DPFTAutomationException("SYSTEM", "AUTO0022E", e);
		}
		macro.setInvokeMethod(action);
		macro.setInvokeMethodArgs(args);
		return macro;
	}

	public DPFTAutomationMacro translateConditionScript(String cndString) throws DPFTRuntimeException {
		if(cndString.indexOf(";") != -1){
			String[] cnds = cndString.split(GlobalConstants.FILE_DELIMETER_SEMICOLON, -1);
			String[] args = {cnds[1]};
			return loadMACRO(cnds[0], GlobalConstants.DPFT_AUTOMATION_MACRO_CONDITION, buildArguments(args));
		}
		String[] args = {cndString};
		return loadMACRO(GlobalConstants.DPFT_AUTOMATION_MACRO_ACTION_CHKCND, GlobalConstants.DPFT_AUTOMATION_MACRO_CONDITION, buildArguments(args));
	}

	public DPFTAutomationMacro translateVarScript(String value) throws DPFTRuntimeException {
		String[] vars = value.split(GlobalConstants.FILE_DELIMETER_SEMICOLON, -1);
		String action = vars[0];
		String[] args = vars[1].split(GlobalConstants.FILE_DELIMETER_SHARP);
		return loadMACRO(action, GlobalConstants.DPFT_AUTOMATION_MACRO_VARS, buildArguments(args));
	}
	
	private Object[] buildArguments(String[] args) throws DPFTRuntimeException{
		if(args == null)
			return null;
		
		Object[] return_args = new Object[args.length];
		int i = 0;
		for(String argv: args){
			return_args[i] = buildArgument(argv);
			i++;
		}
		return return_args;
	}

	private Object buildArgument(String argv) throws DPFTRuntimeException {
		String[] vars = getVariables(argv);
		Object rtnv = replaceVariables(argv, vars);
		return rtnv;
	}

	private Object replaceVariables(String argv, String[] vars) throws DPFTRuntimeException {
		for(String var: vars){
			DPFTAutomationVariablesSet settings = getAutomationVarSetting(var);
			settings.close();
			DPFTAutomationVariables var_setting = (DPFTAutomationVariables) settings.getDbo(0);
			if(var_setting == null)
				return null;
			Object value = var_setting.getValue();
			if(value instanceof String){
				String s = (String) value;
				DPFTLogger.debug(this, "#########Parser : Replacing var = " + var + " with value = " + s);
				argv = argv.replace("$(" + var + ")", s);
			}else{
				return value;
			}
		}
		return argv;
	}

	private DPFTAutomationVariablesSet getAutomationVarSetting(String var) throws DPFTRuntimeException {
		String group_id = ps.getString("group_id");
		if(var.indexOf(".") != -1){
			group_id = var.split(GlobalConstants.FILE_DELIMETER_DOT)[0];
			var = var.split(GlobalConstants.FILE_DELIMETER_DOT)[1];
		}
		
		DPFTAutomationVariablesSet set = (DPFTAutomationVariablesSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("DPFT_AUTOMATION_VARS", "group_id='" + group_id + "' and var='" + var + "'");
		if(set.isEmpty()){
			set.close();
			Object[] params = {var, group_id};
			throw new DPFTAutomationException("SYSTEM", "AUTO0023E", params);
		}
		return set;
	}

	private String[] getVariables(String condition) {
		Pattern pattern = Pattern.compile("\\$\\((.*?)\\)");
		Matcher m = pattern.matcher(condition);
		ArrayList<String> list = new ArrayList<String>();
		while(m.find()){
			String var = m.group(1);
			list.add(var);
			DPFTLogger.debug(this, "Parse : find variables in Arguments = " + var);
		}
		return list.toArray(new String[list.size()]);
	}
}

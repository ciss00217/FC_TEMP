package com.ibm.dpft.engine.core.auto.util.macro;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationCCT;
import com.ibm.dpft.engine.core.auto.dbo.DPFTAutomationCCTSet;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationMacro;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationScheduleParser;
import com.ibm.dpft.engine.core.auto.util.DPFTAutomationScriptTranslator;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTArchiveDbo;
import com.ibm.dpft.engine.core.dbo.DPFTArchiveDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.FileMetaDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTBashRunner;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.dpft.engine.core.util.DPFTcdFTPUtil;

public class MacroAUTO extends DPFTAutomationMacro {
	public int CdFTP(DPFTCSVFileFormatter formatter, DPFTDboSet rs, FileMetaDefDboSet meta) throws DPFTRuntimeException{
		if(rs.isEmpty())
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		
		HashMap<String, String> file_out_list = new HashMap<String, String>();
		HashMap<String, String> file_charset_list = new HashMap<String, String>();
		formatter.format(rs);
		file_out_list = formatter.getFormatFileList();
		file_charset_list = formatter.getFormatFileCharset();
		
		/*Write out files*/
		doFileOut(file_out_list, file_charset_list, meta);
		
		String[] flist = formatter.getFiles();
		DPFTcdFTPUtil ftpUtil = new DPFTcdFTPUtil(meta.getLocalDir(), meta.getRemoteDir(), meta.getCDProfile());
		ftpUtil.doFTP_Out(formatter.getControlFiles(), flist);
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	private int doFileOut(HashMap<String, String> file_out_list, HashMap<String, String> file_charset_list, FileMetaDefDboSet meta) throws DPFTAutomationException, DPFTRuntimeException {
		
		for(String filename: file_out_list.keySet()){
			try{
				File fdir = new File(meta.getLocalDir());
				if(!fdir.exists()){
					fdir.mkdirs();
				}
				File f = new File(meta.getLocalDir() + File.separator + filename);
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(f), file_charset_list.get(filename)));
				out.write(file_out_list.get(filename));
				out.flush();
				out.close();
			}catch(Exception e){
				Object[] params = {filename};
				throw new DPFTAutomationException("SYSTEM","AUTO0005E", params, e);
			}
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	public int Run(DPFTTaskRunner runner){
		runner.start();
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	public int SelectRunnable(DPFTDboSet rs, String type) throws DPFTRuntimeException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTAutomationCCTSet cctSet = (DPFTAutomationCCTSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
										.getDboSet("CCT");
		
		for(int i = 0; i < rs.count(); i++){
			String[] schedule = getSchedule(rs.getDbo(i));
			String name = rs.getDbo(i).getString("name");
			DPFTLogger.debug(this, "Flowchart_name: " + name);
			try {
				if(schedule == null){
					//set CCT without schedule check
					String t = getSelectedType(name, type);
					if(t != null){
						DPFTAutomationCCT cct = (DPFTAutomationCCT) cctSet.add();
						cct.setValue(rs.getDbo(i), t);
					}
				}else{
					DPFTAutomationScheduleParser sparser = null;
					try{
						sparser = new DPFTAutomationScheduleParser(schedule[1], schedule[2], schedule[3], schedule[4], schedule[5]);
					}catch(Exception e){
						Object[] params = {rs.getDbo(i).getString("schedule")};
						throw new DPFTAutomationException("SYSTEM", "AUTO0020E", params, e);
					}
					String t = getSelectedType(name, type);
					if(sparser.IsScheduled() && DPFTUtil.isScheduleTaskActive(name) && t != null){
						DPFTAutomationCCT cct = (DPFTAutomationCCT) cctSet.add();
						cct.setValue(rs.getDbo(i), t);
					}
				}
			}catch (Exception e) {
				throw new DPFTAutomationException("SYSTEM", "AUTO0006E", e);
			}
		}
		cctSet.save();
		cctSet.close();
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	
	private String getSelectedType(String name, String type) {
		if(type.indexOf(GlobalConstants.FILE_DELIMETER_COMMA) != -1){
			String[] types = type.split(GlobalConstants.FILE_DELIMETER_COMMA);
			for(String t: types){
				if(name.indexOf("_" + t + "_") != -1)
					return t;
			}
			return null;
		}
		if(name.indexOf("_" + type + "_") != -1)
			return type;
		return null;
	}

	private String[] getSchedule(DPFTDbo dbo) {
		String schedule = dbo.getString("schedule");
		return (schedule != null) ? schedule.split(" +"):null;
	}


	public int Archive(String dbname, String source_tbl, String target_tbl) throws DPFTRuntimeException, SQLException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTDboSet sSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname)).getDboSet(source_tbl);
//		DPFTArchiveDboSet tSet = (DPFTArchiveDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname)).getDboSet("TOP 1 * ", target_tbl, "");
		DPFTArchiveDboSet tSet = (DPFTArchiveDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname)).getDboSet(target_tbl, "rownum<=1");
		if(sSet.isEmpty())
			//Do nothing, return Normal
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		String timestamp = DPFTUtil.getCurrentTimeStampAsString();
		for(int i = 0; i < sSet.count(); i++){
			DPFTArchiveDbo t = (DPFTArchiveDbo) tSet.add();
			t.setArchiveInfo(sSet.getDbo(i), timestamp);
		}
		tSet.setRefresh(false);
		tSet.save();
		sSet.close();
		tSet.close();
		return Truncate(dbname, source_tbl);
	}
	
	/**
	 * 
	 * @param dbname
	 * @param source_tbl
	 * @param target_tbl
	 * @param source_query
	 * @return
	 * @throws DPFTRuntimeException
	 * @throws SQLException
	 */
	public int ArchiveByQuery(String dbname, String source_tbl, String target_tbl, String source_query)
			throws DPFTRuntimeException, SQLException {
		DPFTLogger.debug(this, "@@@Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTDboSet sSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname)).getDboSet(source_tbl,
				source_query);
		// rownum<=1 可能避免查到多張同名table
		DPFTDboSet tSet = DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getDBConfig(dbname)).getDboSet(target_tbl, "rownum<=1");

		if (sSet.isEmpty())
			// Do nothing, return Normal
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		String timestamp = DPFTUtil.getCurrentTimeStampAsString();
		for (int i = 0; i < sSet.count(); i++) {
			DPFTDbo t =  tSet.add();
			
			t.setValue(sSet.getDbo(i));
			t.setValue("ARCHIVE_TIME", timestamp);
		}
		tSet.setRefresh(false);
		tSet.save();
		sSet.close();
		tSet.close();

		return this.Delete(dbname, source_tbl, source_query);
	}

	public int Delete(String dbname, String source_tbl, String source_query) throws DPFTRuntimeException, SQLException {
		DPFTLogger.info(this, "@@@Delete" + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname));

		DPFTDboSet deleteSet = connector.getDboSet(source_tbl, source_query);

		try {
//			deleteSet.deleteAll();
//			deleteSet.save();
		} catch (Exception e) {
			Object[] params = { source_tbl };
			throw new DPFTAutomationException("SYSTEM", "DELETE_", params);
		} finally {
			connector.close();

		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;

	}
	
	
	public int Truncate(String dbname, String source_tbl) throws DPFTRuntimeException, SQLException{
		DPFTLogger.debug(this, "Truncate" + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname));
		try {
			connector.truncate(source_tbl);
		} catch (SQLException e) {
			connector.close();
			Object[] params = {source_tbl};
			throw new DPFTAutomationException("SYSTEM", "AUTO0026E", params);
		} finally {
			if (connector != null) {
				connector.close();
			}
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	public int execSQL(String dbname, String script) throws DPFTRuntimeException, SQLException {
		DPFTLogger.debug(this, "invode " + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getDBConfig(dbname));
		try {
			connector.executeUpdateDelete(script);
		} catch(SQLException e) {
			connector.close();
			Object[] params = {script};
			throw new DPFTAutomationException("SYSTEM", "AUTO0026E", params);
		} finally {
			if (connector != null) {
				connector.close();
			}
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	public int RunFlowChart(String cmd, String type, String logfile, String seq_run_flg, String failed_break_flg) throws DPFTRuntimeException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		DPFTAutomationCCTSet cctSet = (DPFTAutomationCCTSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("CCT", "flowcharttype='" + type + "'");
		String[] run_flowchart_orders = cctSet.getRunnableFlowChartByType(type);
		for (String i : run_flowchart_orders) {
			DPFTLogger.info(this, i);
		}
		
		int rc = 0;
		try{
			if(seq_run_flg.equalsIgnoreCase("y"))
				rc = _runFlowChartByOrder(cmd, run_flowchart_orders, cctSet, logfile, type, failed_break_flg.equalsIgnoreCase("y"));
			else
				rc = _runFlowChart(cmd, run_flowchart_orders, type, cctSet, logfile);
		}catch(DPFTRuntimeException ex){
			Object[] params = {type};
			throw new DPFTAutomationException("SYSTEM", "AUTO0004E", params, ex);
		}
		cctSet.close();
		return rc;
	}
	
	private int _runFlowChart(String cmd, String[] run_flowchart_orders, String flowchartype, DPFTAutomationCCTSet cctSet, String logfile) throws DPFTRuntimeException {
		for(String filename: run_flowchart_orders){
			DPFTBashRunner br = new DPFTBashRunner();
			br.setBashCmd(buildFlowchartCmd(filename, cmd));
			br.setLogFile(logfile + "_" + DPFTUtil.getCurrentTimeStampAsString().substring(0, GlobalConstants.DFPT_DATE_FORMAT.length()) + "_" + filename);
			try {
				Object[] params1 = {cctSet.getDbo(getIndiceByFName(filename, cctSet)).getString("name")};
//				String cmp_owner = DPFTUtil.getCampaignOwnerEmail(cctSet.parseCampaignCode(flowchartype,
//						cctSet.getDbo(getIndiceByFName(filename, cctSet)).getString("name")));
//				DPFTUtil.pushNotification(cmp_owner, new DPFTMessage("SYSTEM", "DPFT0043I", params1));
				int rc = br.execute();
				if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL){
//					cctSet.getDbo(idx).setValue("cct_status", GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_INPRG);
					setCCTStatusByFName(filename, cctSet, GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_INPRG);
				}else{
//					cctSet.getDbo(idx).setValue("cct_status", GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_CMD_FAILED);
					setCCTStatusByFName(filename, cctSet, GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_CMD_FAILED);
//					DPFTUtil.pushNotification(
//							cmp_owner,
//							new DPFTMessage("SYSTEM", "DPFT0042I", params1)
//					);
				}
				cctSet.save();
			} catch (IOException | InterruptedException e) {
				Object[] params = {br.getBashCmd()};
				throw new DPFTAutomationException("SYSTEM", "AUTO0007E", params);
			}
		}
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}


	private int getIndiceByFName(String filename, DPFTAutomationCCTSet cctSet) throws DPFTRuntimeException {
		int rtnV = 0;
		for(int i = 0; i < cctSet.count(); i++){
			if(cctSet.getDbo(i).getString("filename").equals(filename)){
				return i;
			}
		}
		return rtnV;
	}

	private int _runFlowChartByOrder(String cmd, String[] run_flowchart_orders, DPFTAutomationCCTSet cctSet,
			String logfile, String type, boolean failed_break) throws DPFTRuntimeException {
		if(run_flowchart_orders.length == 0)
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;

		int i = 0;
		do{
			String current_gk = run_flowchart_orders[i];
			DPFTBashRunner br = new DPFTBashRunner();
			br.setBashCmd(buildFlowchartCmd(current_gk, cmd));
			br.setLogFile(logfile + "_" + DPFTUtil.getCurrentTimeStampAsString().substring(0, GlobalConstants.DFPT_DATE_FORMAT.length()) + "_" + current_gk);
			try {
				int rc = br.execute();
				if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL){
					setCCTStatusByFName(current_gk, cctSet, GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_INPRG);
				}else{
					setCCTStatusByFName(current_gk, cctSet, GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_CMD_FAILED);
				}
				cctSet.save();
			} catch (IOException | InterruptedException e) {
				Object[] params = {br.getBashCmd()};
				throw new DPFTAutomationException("SYSTEM", "AUTO0007E", params);
			}
			
			try {
				while(true){
					int rc = isCurrentSessionFinished(type);
					if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE)
						break;
					//waiting
					Thread.sleep(10000);
				}
			} catch (Exception e) {
				Object[] params = {current_gk};
				DPFTAutomationException ex = new DPFTAutomationException("SYSTEM", "AUTO0008E", params, e);
				if(failed_break)
					throw ex;
				else
					ex.handleException();
			}
			i++;
		}while(i < run_flowchart_orders.length);
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}


	private void setCCTStatusByFName(String current_gk, DPFTAutomationCCTSet cctSet,
			String cct_status) throws DPFTRuntimeException {
		for(int i = 0; i < cctSet.count(); i++){
			if(cctSet.getDbo(i).getString("filename").equals(current_gk)){
				cctSet.getDbo(i).setValue("cct_status", cct_status);
			}
		}
	}

	private int isCurrentSessionFinished(String type) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, DPFTRuntimeException {
		DPFTAutomationScriptTranslator trs = new DPFTAutomationScriptTranslator(getProcess());
		return trs.translateScript("isFlowChartFinished", "AUTO", "$(CCT_RESULT)#" + type).invoke();
	}


	public int isFlowChartFinished(DPFTDboSet rs, String flowchart_type) throws DPFTRuntimeException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		if(rs.isEmpty()){
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		}
		
		DPFTAutomationCCTSet cctSet = (DPFTAutomationCCTSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("CCT", "flowcharttype='" + flowchart_type + "'");
		
		for(int i = 0; i < cctSet.count(); i++){
			DPFTAutomationCCT cct = (DPFTAutomationCCT) cctSet.getDbo(i);
			if(!cct.getString("flowcharttype").equalsIgnoreCase(flowchart_type))
				continue;
			cct.updateInfo(rs);
		}
		cctSet.save();
		int rc = cctSet.isAllFlowChartFinished(flowchart_type);
		if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE){
			DPFTLogger.debug(this, "All Flowchart is finished...");
		}else if(rc == GlobalConstants.DPFT_AUTOMATION_PS_RC_WAITING){
			DPFTLogger.debug(this, "Flowchart is still Running...");
		}
		cctSet.close();
		return rc;
	}


	private String buildFlowchartCmd(String filename, String cmd) {
		cmd = cmd.replace("<FILENAME>", filename);
		return cmd;
	}
	
	public int insertInteractPerf(DPFTDboSet rs) throws DPFTRuntimeException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		if(rs.isEmpty()){
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		}
		DPFTDboSet perfSet = (DPFTDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("S1_TP2","rownum<=1");
		for (int i=0 ; i < rs.count() ; i++) {
			DPFTDbo perf = perfSet.add();
			perf.setValue(rs.getDbo(i));
		}
		perfSet.setRefresh(false);
		perfSet.save();
		perfSet.close();
		rs = null;
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}
	
	public int insertInteractPrp(DPFTDboSet rs, String tbl_name) throws DPFTRuntimeException{
		DPFTLogger.debug(this, "Invoke " + this.getInvokeMethod() + " in MACRO AUTO");
		if(rs.isEmpty()){
			return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
		}
		DPFTDboSet perfSet = (DPFTDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet(tbl_name,"rownum<=1");
		for (int i=0 ; i < rs.count() ; i++) {
			DPFTDbo perf = perfSet.add();
			perf.setValue(rs.getDbo(i));
		}
		perfSet.setRefresh(false);
		perfSet.save();
		perfSet.close();
		rs.close();
		perfSet = null;
		rs = null;
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_NORMAL;
	}

}

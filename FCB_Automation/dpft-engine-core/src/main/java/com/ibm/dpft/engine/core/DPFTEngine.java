package com.ibm.dpft.engine.core;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTPrioritySettingDboSet;
import com.ibm.dpft.engine.core.dbo.SystemPropDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.taskplan.DPFTDataInboundWatcher;
import com.ibm.dpft.engine.core.taskplan.DPFTDataOutboundPeriodicWatcher;
import com.ibm.dpft.engine.core.taskplan.DPFTDataOutboundWatcher;
import com.ibm.dpft.engine.core.taskplan.DPFTInstaDataInboundWatcher;
import com.ibm.dpft.engine.core.taskplan.DPFTInstaDataOutboundWatcher;
import com.ibm.dpft.engine.core.taskplan.DPFTInstaFileWatcher;
import com.ibm.dpft.engine.core.taskplan.DPFTTaskPlan;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTEngine {
	private DPFTConfig cfg = null;
	//private static DPFTConnector connObj = null;
	private DPFTScheduler scheduler = null;
	private DPFTTaskRunnerManager taskrunnerMgr = null;
	private static Properties prop = null;
	private String stat = GlobalConstants.DPFT_ENGINE_STAT_INIT;
	private ArrayList<DPFTTaskPlan> rtplist = new ArrayList<DPFTTaskPlan>();
	private ArrayList<DPFTTaskPlan> utplist = new ArrayList<DPFTTaskPlan>();
	private static DPFTPrioritySettingDboSet pSet = null;

	public DPFTEngine() throws DPFTRuntimeException {
		DPFTLogger.info(this, "Engine instantiate Read System properties from config file...");
		prop = getConfigProperites();
		cfg = DPFTUtil.getSystemDBConfig();
	}

	public void initialize() throws DPFTRuntimeException {

		if (cfg == null)
			return;

		DPFTLogger.info(this, "Engine instantiate...");
		DPFTLogger.info(this, "Engine try to connect to DB ...");
		//connObj = DPFTConnectionFactory.initDPFTConnector(cfg, false);
		DPFTConnector.setSystemDBConnectorInstance(cfg);
		DPFTLogger.info(this, "Engine successfully connected to DB...");

		DPFTLogger.info(this, "Engine load Remote properties from System Table...");
		try {
			loadRemoteSystemProperties();
		} catch (Throwable e) {
			DPFTLogger.info(this, "====>" + e.toString());
			DPFTLogger.error(this, "====>" + e.toString(),new Exception(e));
		}
		try {
			loadPriorityCodeSetting();
		} catch (Throwable e) {
			//DPFTLogger.error(this, "", e);
		}
		DPFTLogger.info(this, "Engine successfully load Remote properties...");

		DPFTLogger.info(this, "Engine Scheduler instantiate...");
		scheduler = new DPFTScheduler(cfg);
		for (DPFTTaskPlan tp : getEngineDefinedTaskPlanList()) {
			scheduler.registerTaskPlan(tp, true);
		}
		scheduler.registerTaskPlanFromDB();
		DPFTLogger.info(this, "Engine Scheduler successfully instantiated...");

		DPFTLogger.info(this, "Engine tasks runner manager instantiate...");
		taskrunnerMgr = new DPFTTaskRunnerManager(scheduler);
		taskrunnerMgr.initialize(stat);
		DPFTLogger.info(this, "Engine tasks runner manager successfully instantiate...");

	}

	private void loadPriorityCodeSetting() throws DPFTRuntimeException {
		if (pSet == null) {
			pSet = (DPFTPrioritySettingDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_PRIORITY_CODE_DEF", "active=1");
			pSet.load();
			pSet.close();
		}
	}

	private DPFTTaskPlan[] getEngineDefinedTaskPlanList() {
		if (utplist.size() != 0) {
			rtplist.addAll(utplist);
		}
		rtplist.add(new DPFTDataInboundWatcher(GlobalConstants.DPFT_SYS_SCHEDULE_RUNNER_ID));
		rtplist.add(new DPFTDataOutboundWatcher(GlobalConstants.DPFT_SYS_OBND_RUNNER_ID));
		rtplist.add(new DPFTInstaDataInboundWatcher(GlobalConstants.DPFT_SYS_RES_INSTA_SYS_SCHEDULE_RUNNER_ID));
		rtplist.add(new DPFTInstaDataOutboundWatcher(GlobalConstants.DPFT_SYS_RES_INSTA_SYS_OBND_RUNNER_ID));
		
		// rtplist.add(new
		// DPFTResRemoteFileWatcher(GlobalConstants.DPFT_SYS_RES_GETFILE_RUNNER_ID));
		// rtplist.add(new
		// DPFTResLocalFileWatcher(GlobalConstants.DPFT_SYS_RES_READFILE_RUNNER_ID));
		
		
		//處理回應檔
		rtplist.add(new DPFTInstaFileWatcher(GlobalConstants.DPFT_SYS_RES_INSTA_FILETRIGGER_RUNNER_ID));
		
		//定時出檔
		rtplist.add(new DPFTDataOutboundPeriodicWatcher(GlobalConstants.DPFT_SYS_OBND_PERIODIC_RUNNER_ID));
		return rtplist.toArray(new DPFTTaskPlan[rtplist.size()]);

		// return utplist.toArray(new DPFTTaskPlan[utplist.size()]);
	}

	private void loadRemoteSystemProperties() throws DPFTRuntimeException {

		SystemPropDboSet propSet = (SystemPropDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("DPFT_SYSTEM_PROP", "active=1");
		for (int i = 0; i < propSet.count(); i++) {
			String rprop = propSet.getDbo(i).getString("prop");
			String value = propSet.getDbo(i).getString("value");
			if (prop.containsKey(rprop)) {
				prop.remove(rprop);
			}
			if (!prop.containsKey(rprop)) {
				//!!!!!!!
//				DPFTLogger.debug(this, "Loading Remote Properties " + rprop + ", value = " + value);
				if (value == null)
					prop.put(rprop, "");
				else
					prop.put(rprop, value);
			}
		}
		propSet.close();
	}

	public static DPFTPrioritySettingDboSet getPriorityCodeSetting() {
		return pSet;
	}

	public static String getSystemProperties(String key) {
		return prop.getProperty(key);
	}
	
	public static void setSystemProperty(String key, String value) {
		if (prop == null) {
			prop = new Properties();
		}
		prop.put(key, value);
	}

	private Properties getConfigProperites() throws DPFTRuntimeException {
		if (prop != null)
			return prop;
		InputStream input = null;
		try {
			input = this.getClass().getClassLoader().getResourceAsStream("config.properties");
			Properties p = new Properties();
			p.load(input);
			return p;
		} catch (IOException e) {
			throw new DPFTRuntimeException("SYSTEM", "DPFT0034E");
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					throw new DPFTRuntimeException("SYSTEM", "DPFT0034E");
				}
			}
		}
	}

	public void start() {

		DPFTLogger.info(this, "Engine Starting...");
		taskrunnerMgr.exec();
		stat = GlobalConstants.DPFT_ENGINE_STAT_RUN;
		DPFTLogger.info(this, "Engine successfully started...");
	}

	public void stop() {

		DPFTLogger.info(this, "Engine Stopping...");

	}

	public void startAutomationMode() throws DPFTRuntimeException {
		DPFTLogger.info(this, "Engine Switching to Automation Mode...");
		taskrunnerMgr.stopAllRunners();
		taskrunnerMgr.clearDeamonsQueue();
		resetSystemConnectorPool();
		stat = GlobalConstants.DPFT_ENGINE_STAT_PASSIVE;
		DPFTLogger.info(this, "Engine initializing system deamons for Automation Mode...");
		taskrunnerMgr.initialize(stat);
		DPFTLogger.info(this, "Engine Starting in Automation Mode...");
		taskrunnerMgr.exec();
		DPFTLogger.info(this, "Engine successfully started in Automation Mode...");

		updateSystemProp(GlobalConstants.DPFT_SYS_PROP_AUTO_ENABLE_TIME, getNextAutomationTrgTime());

	}

	public static void updateSystemProp(String p, String value) throws DPFTRuntimeException {
		DPFTLogger.info(DPFTEngine.class, "updateSystemProp:" + p + " value:" + value);
		SystemPropDboSet propSet = (SystemPropDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("DPFT_SYSTEM_PROP",
				"prop='" + p + "' and active=1");
		if (propSet.isEmpty()) {
			propSet.close();
			Object[] params = { p };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0035E", params);
		}
		propSet.getDbo(0).setValue("value", value);
		propSet.save();
		propSet.close();
		prop.put(p, value);
		DPFTLogger.info(DPFTEngine.class, "getSystemProp:" + p + " value:" + prop.getProperty(p));
	}

	private String getNextAutomationTrgTime() throws DPFTRuntimeException {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		Calendar cal = Calendar.getInstance();
		try {
			Date trg_time = DPFTUtil
					.convertDateString2DateObject(getSystemProperties(GlobalConstants.DPFT_SYS_PROP_AUTO_ENABLE_TIME));
			cal.setTime(trg_time);
			cal.add(Calendar.DAY_OF_MONTH, 1);
			return sdf.format(cal.getTime());
		} catch (Exception e) {
			throw new DPFTRuntimeException("SYSTEM", "AUTO0025E", e);
		}
	}

	public boolean isAutomationModeActive() throws DPFTRuntimeException {
		Date current_time = new Date();
		try {
			loadRemoteSystemProperties();
			Date trg_time = DPFTUtil
					.convertDateString2DateObject(getSystemProperties(GlobalConstants.DPFT_SYS_PROP_AUTO_ENABLE_TIME));
			DPFTLogger
					.info(this,
							"isAutomationModeActive(): "
									+ (current_time.after(trg_time)
											&& !stat.equals(GlobalConstants.DPFT_ENGINE_STAT_PASSIVE)
											&& DPFTEngine.getSystemProperties(GlobalConstants.DPFT_SYS_PROP_AUTO_ENABLE)
													.equalsIgnoreCase("y"))
									+ " and stat:" + stat + " trg_time:" + trg_time);
			
			if (current_time.after(trg_time) && !stat.equals(GlobalConstants.DPFT_ENGINE_STAT_PASSIVE) && DPFTEngine
					.getSystemProperties(GlobalConstants.DPFT_SYS_PROP_AUTO_ENABLE).equalsIgnoreCase("y")) {
				DPFTLogger.info(this, "isAutomationModeActive():true stat:" + stat);
			}
			return current_time.after(trg_time) && !stat.equals(GlobalConstants.DPFT_ENGINE_STAT_PASSIVE)
					&& DPFTEngine.getSystemProperties(GlobalConstants.DPFT_SYS_PROP_AUTO_ENABLE).equalsIgnoreCase("y");
		} catch (Exception e) {
			throw new DPFTRuntimeException("SYSTEM", "AUTO0025E", e);
		}
	}

	public void add2UserDefinedTaskPlanList(String classname)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException {
		String id = "UDF" + (utplist.size() + 1);
		utplist.add((DPFTTaskPlan) Class.forName(classname).getConstructor(String.class).newInstance(id));
	}

	public boolean isResume() {
		if (taskrunnerMgr.getAutomationManager() == null)
			return false;
		DPFTLogger.info(this, "isResume:" + !taskrunnerMgr.getAutomationManager().isActive());
		return !taskrunnerMgr.getAutomationManager().isActive();
	}

	public void resume() throws DPFTRuntimeException {
		DPFTLogger.info(this, "Engine Resuming to Default Running Mode...");
		taskrunnerMgr.stopAllRunners();
		taskrunnerMgr.clearDeamonsQueue();
		resetSystemConnectorPool();
		stat = GlobalConstants.DPFT_ENGINE_STAT_RUN;
		DPFTLogger.info(this, "Engine initializing system deamons for Default Running Mode...");
		taskrunnerMgr.initialize(stat);
		DPFTLogger.info(this, "Engine Starting...");
		taskrunnerMgr.exec();
		DPFTLogger.info(this, "Engine successfully started...");

	}

	private void resetSystemConnectorPool() throws DPFTRuntimeException {
		DPFTConnector.closeAllConnection();
		DPFTConnector.clearClosedConnection();
		//connObj = DPFTConnectionFactory.initDPFTConnector(cfg, false);
		DPFTConnector.setSystemDBConnectorInstance(DPFTUtil.getSystemDBConfig());
	}

}

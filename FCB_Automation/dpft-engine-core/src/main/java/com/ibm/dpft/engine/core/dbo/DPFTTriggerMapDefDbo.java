package com.ibm.dpft.engine.core.dbo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.exception.DPFTDboException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTTriggerMapDefDbo extends DPFTDbo {
	private static final Pattern tt_pattern_month = Pattern.compile(
			"(ED([\\-\\+]\\d{1,2})?|FD([\\-\\+]\\d{1,2})?|\\d{2})((0[0-9]|1[0-9]|2[0-3])([0-5][0-9])([0-5][0-9]))");
	private static final Pattern tt_pattern_day = Pattern.compile("(0[0-9]|1[0-9]|2[0-3])([0-5][0-9])([0-5][0-9])");

	private Date current_time = null;
	private String trg_time_string = null;

	public DPFTTriggerMapDefDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisSet) {
		super(dboname, data, thisSet);

	}

	public boolean canInvoke() throws DPFTRuntimeException {
		current_time = new Date();
		try {
			if (this.isNull("frequency")) {
				return false;
			} else if (this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_DAILY)) {
				return passTriggerTime() && pass24Hours();
			} else if (this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_MONTHLY)) {
				return passTriggerTime() && pass1Month();
			} else if (this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_HOURLY)) {
				return passHours();
			}
		} catch (Exception e) {
			DPFTDboException ex = new DPFTDboException("SYSTEM", "DPFT0028E", e);
			ex.handleException();
		}
		return false;
	}

	private boolean pass1Month() throws ParseException {
		if (this.isNull("last_active_time"))
			return true;
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		Date last_active_time = sdf.parse(this.getString("last_active_time"));
		Calendar c1 = Calendar.getInstance();
		c1.setTime(last_active_time);
		Calendar c2 = Calendar.getInstance();
		c2.setTime(current_time);
		if (c2.get(Calendar.MONTH) > c1.get(Calendar.MONTH))
			return true;
		else
			return c2.get(Calendar.YEAR) > c1.get(Calendar.YEAR);
	}

	private boolean pass24Hours() throws ParseException {
		if (this.isNull("last_active_time"))
			return true;
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		Date last_active_time = sdf.parse(this.getString("last_active_time"));
		long diff = current_time.getTime() - last_active_time.getTime();
		long hour_diff = diff / (3600 * 1000);
		if (hour_diff < 24) {
			DPFTLogger.debug(this, "Task Plan : " + this.getString("planid") + " remaining " + (24 - hour_diff)
					+ " hours to be invoked...");
			return false;
		}
		DPFTLogger.info(this, "Daily Task Plan : " + this.getString("planid") + " is preparing to run...");
		return true;
	}

	private boolean passHours() throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		trg_time_string = sdf.format(current_time);
		if (this.isNull("last_active_time"))
			return true;

		Date last_active_time = sdf.parse(this.getString("last_active_time"));
		long diff = current_time.getTime() - last_active_time.getTime();
		long hour_diff = diff / (3600 * 1000);
		if (hour_diff < 1) {
			DPFTLogger.debug(this, "Task Plan : " + this.getString("planid") + " remaining " + diff / (60 * 1000)
					+ " minutes to be invoked...");
			return false;
		}
		DPFTLogger.info(this, "Daily Task Plan : " + this.getString("planid") + " is preparing to run...");
		return true;
	}

	private boolean passTriggerTime() throws ParseException, DPFTRuntimeException {
		String trg_time = this.getString("trg_time");
		Matcher m = getMatcher(trg_time);
		if (m == null) {
			Object[] params = { this.getString("frequency") };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0048E", params);
		}

		if (!m.matches()) {
			Object[] params = { trg_time };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0048E", params);
		}

		if (this.isNull("frequency") || this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_DAILY)) {
			// process daily trigger time setting
			return _passDailyTriggerTime(trg_time);
		} else if (this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_MONTHLY)) {
			// process monthly trigger time setting
			String day_setting = m.group(1);
			String time_setting = m.group(4);
			return _passMonthlyTriggerTime(day_setting, time_setting);
		}
		return false;
	}

	private boolean _passMonthlyTriggerTime(String day_setting, String time_setting) throws ParseException {
		int offset = 0;
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		if (day_setting.indexOf(GlobalConstants.DPFT_OBND_TT_MONTH_ENDDAY) != -1) {
			// setting indicate month end day
			if (day_setting.indexOf("+") != -1)
				offset = Integer.valueOf(day_setting.substring(day_setting.indexOf("+") + 1));
			if (day_setting.indexOf("-") != -1)
				offset = Integer.valueOf(day_setting.substring(day_setting.indexOf("-")));

			Calendar c = Calendar.getInstance();
			c.setTime(current_time);
			c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH) + offset);
			trg_time_string = sdf.format(c.getTime()) + time_setting;

		} else if (day_setting.indexOf(GlobalConstants.DPFT_OBND_TT_MONTH_FIRSTDAY) != -1) {
			// setting indicate month start day
			if (day_setting.indexOf("+") != -1)
				offset = Integer.valueOf(day_setting.substring(day_setting.indexOf("+") + 1));
			if (day_setting.indexOf("-") != -1)
				offset = Integer.valueOf(day_setting.substring(day_setting.indexOf("-")));

			Calendar c = Calendar.getInstance();
			c.setTime(current_time);
			c.set(Calendar.DAY_OF_MONTH, c.getActualMinimum(Calendar.DAY_OF_MONTH) + offset);
			trg_time_string = sdf.format(c.getTime()) + time_setting;

		} else {
			// setting indicate month date
			Calendar c = Calendar.getInstance();
			c.setTime(current_time);
			c.set(Calendar.DAY_OF_MONTH, Integer.valueOf(day_setting));
			trg_time_string = sdf.format(c.getTime()) + time_setting;
		}
		SimpleDateFormat sdf1 = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		if (current_time.after(sdf1.parse(trg_time_string))) {
			DPFTLogger.debug(this, "Current time of the month pass Trigger Time...");
			return true;
		}
		DPFTLogger.debug(this, "Current time of the month do not pass Trigger Time...");
		return false;
	}

	private boolean _passDailyTriggerTime(String trg_time) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		trg_time_string = sdf.format(current_time) + trg_time;
		SimpleDateFormat sdf1 = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		if (current_time.after(sdf1.parse(trg_time_string))) {
			DPFTLogger.debug(this, "Current time of the day pass Trigger Time...");
			return true;
		}
		DPFTLogger.debug(this, "Current time of the day do not pass Trigger Time...");
		return false;
	}

	private Matcher getMatcher(String argv) {
		if (this.isNull("frequency") || this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_DAILY)) {
			return tt_pattern_day.matcher(argv);
		} else if (this.getString("frequency").equals(GlobalConstants.DPFT_OBND_FEQ_MONTHLY)) {
			return tt_pattern_month.matcher(argv);
		}
		return null;
	}

	public String getDataSelectCriteria() {
		// to_timestamp(process_time, 'YYYYMMDDHH24MISS') between
		// to_timestamp('20160505192000', 'YYYYMMDDHH24MISS') and
		// to_timestamp('20160506192000', 'YYYYMMDDHH24MISS')
		StringBuilder sb = new StringBuilder();
//		sb.append("to_timestamp(process_time, '").append(GlobalConstants.ORA_DB_TIME_FORMAT)
//		.append("') between to_timestamp('").append(this.getString("last_active_time")).append("', '").append(GlobalConstants.ORA_DB_TIME_FORMAT)
//		.append("') and to_timestamp('").append(trg_time_string).append("', '").append(GlobalConstants.ORA_DB_TIME_FORMAT)
//		.append("') and process_status='").append(GlobalConstants.O_DATA_OUTPUT).append("'");
		sb.append("process_time >= '").append(this.getString("last_active_time")).append("' and process_time <= '")
				.append(trg_time_string).append("' and process_status='").append(GlobalConstants.O_DATA_OUTPUT)
				.append("'");
		return sb.toString();
	}

	public String getDataSelectCriteria(String process_status) {
		StringBuilder sb = new StringBuilder();
		sb.append("to_timestamp(process_time, '").append(GlobalConstants.ORA_DB_TIME_FORMAT)
				.append("') between to_timestamp('").append(this.getString("last_active_time")).append("', '")
				.append(GlobalConstants.ORA_DB_TIME_FORMAT).append("') and to_timestamp('").append(trg_time_string)
				.append("', '").append(GlobalConstants.ORA_DB_TIME_FORMAT).append("') and process_status='")
				.append(process_status).append("'");
		return sb.toString();
	}

	public String getTrgTimeString() {
		return trg_time_string;
	}

	public DPFTTriggerMapDefDboSet getControlTableRecords() throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		sb.append("chal_name='").append(this.getString("chal_name")).append("' and planid='")
				.append(this.getString("planid")).append("' and active=0");
		DPFTTriggerMapDefDboSet rtnSet = (DPFTTriggerMapDefDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet(this.getDboName(), sb.toString());
		rtnSet.setParent(this);
		return rtnSet;
	}
}

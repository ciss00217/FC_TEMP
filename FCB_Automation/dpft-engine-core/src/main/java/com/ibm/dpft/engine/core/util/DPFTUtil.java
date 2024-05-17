package com.ibm.dpft.engine.core.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.auto.util.macro.MacroAUTO;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTNotificationDbo;
import com.ibm.dpft.engine.core.dbo.DPFTNotificationDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.dbo.UnicaCampaignDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTUtil {

	public static DPFTConfig getSystemDBConfig() {
		return getDBConfig("sys");
	}

	public static DPFTConfig getDBConfig(String profile) {
		DPFTConfig config = new DPFTConfig();
		config.setDBtype(DPFTEngine.getSystemProperties(profile + ".db.type"));
		config.setDBhost(DPFTEngine.getSystemProperties(profile + ".db.host"));
		config.setDBport(DPFTEngine.getSystemProperties(profile + ".db.port"));
		config.setDBServiceName(DPFTEngine.getSystemProperties(profile + ".db.servicename"));
		config.setSid(DPFTEngine.getSystemProperties(profile + ".db.sid"));
		config.setDBUserName(DPFTEngine.getSystemProperties(profile + ".db.user"));
		config.setDBPassword(DPFTEngine.getSystemProperties(profile + ".db.password"));
		return config;
	}

	public static String getFKQueryString(DPFTDbo dbo) {
		if (dbo.isNull("camp_code") || dbo.isNull("timestamp"))
			return null;

		String cc = dbo.getString("camp_code");
		String t = dbo.getString("timestamp");
		StringBuilder sb = new StringBuilder();
		sb.append("camp_code='").append(cc).append("' and timestamp='").append(t).append("'");
		return sb.toString();
	}

	public static DPFTTriggerMapDefDboSet getObndTriggerMap() throws DPFTRuntimeException {
		/* get channel outbound Task Plan Mapping */
		DPFTTriggerMapDefDboSet mapSet = (DPFTTriggerMapDefDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("DPFT_OBND_TP_DEF", "active=1");
		return mapSet;
	}

	public static DPFTTriggerMapDefDboSet getIbndTriggerMap() throws DPFTRuntimeException {
		/* get channel inbound Task Plan Mapping */
		DPFTTriggerMapDefDboSet mapSet = (DPFTTriggerMapDefDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("DPFT_IBND_TP_DEF", "active=1");
		return mapSet;
	}

	public static String getCurrentTimeStampAsString() {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		return sdf.format(new Date());
	}

	public static String getCurrentDateAsString() {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		return sdf.format(new Date());
	}

	public static String getCurrentDateHourAsString() {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIMEHOUR_FORMAT);
		return sdf.format(new Date());
	}

	public static Date convertDateString2DateObject(String dstring) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		return sdf.parse(dstring);
	}

	/**
	 * 全型轉半型
	 * 
	 * @param source
	 * @return
	 */
	public static String convertToHalfWidth(final String source) {
		if (null == source) {
			return null;
		}

		char[] charArray = source.toCharArray();
		for (int i = 0; i < charArray.length; i++) {
			int ic = (int) charArray[i];

			if (ic >= 65281 && ic <= 65374) {
				charArray[i] = (char) (ic - 65248);
			} else if (ic == 12288) {
				charArray[i] = (char) 32;
			}

		}
		return new String(charArray);
	}

	/**
	 * 半型轉全型
	 * 
	 * @param source
	 * @return
	 */
	public static String convertToFullWidth(final String source) {
		if (null == source) {
			return null;
		}

		char[] charArray = source.toCharArray();
		for (int i = 0; i < charArray.length; i++) {
			int ic = (int) charArray[i];

			if (ic >= 33 && ic <= 126) {
				charArray[i] = (char) (ic + 65248);
			} else if (ic == 32) {
				charArray[i] = (char) 12288;
			}

		}
		return new String(charArray);
	}

	public static String convertToString(String[] array, boolean needQuote) {
		StringBuilder sb = new StringBuilder();
		for (String s : array) {
			if (needQuote)
				sb.append("'");
			sb.append(s);
			if (needQuote)
				sb.append("'");
			sb.append(",");
		}
		return sb.substring(0, sb.length() - 1);
	}

	public static boolean isScheduleTaskActive(String name) throws ParseException {
		String[] info = name.split("_");
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATE_FORMAT);
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(info[0]));
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date st = cal.getTime();

		cal.setTime(sdf.parse(info[1]));
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date end = cal.getTime();

		cal.setTime(new Date());
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date today = cal.getTime();

		if (today.equals(st) || today.equals(end))
			return true;
		return today.after(st) && today.before(end);
	}

	public static String convertDateObject2DateString(Date date, String format) {
		if (date == null)
			return null;

		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}

	public static String getSupportedFormat(String datestring) {
		if (datestring.length() == GlobalConstants.DFPT_DATETIME_FORMAT.length()) {
			return GlobalConstants.DFPT_DATETIME_FORMAT;
		} else if (datestring.length() == GlobalConstants.DFPT_DATE_FORMAT.length()) {
			return GlobalConstants.DFPT_DATE_FORMAT;
		} else if (datestring.length() == GlobalConstants.DFPT_DATETIME_FORMAT_2.length()) {
			return GlobalConstants.DFPT_DATETIME_FORMAT_2;
		}
		return null;
	}

	public static void pushNotification(String notifier, DPFTMessage msg) throws DPFTRuntimeException {
		if (notifier == null)
			notifier = msg.getMsgNotifier();
		else
			notifier = notifier + GlobalConstants.FILE_DELIMETER_COMMA + msg.getMsgNotifier();
		pushNotification(notifier, msg.getMessage(), msg.getMessage());
	}

	public static void pushNotification(DPFTMessage msg) throws DPFTRuntimeException {
		pushNotification(msg.getMsgNotifier(), msg.getMessage(), msg.getMessage());
	}

	public static void pushNotification(String notifier, String title, String msg_body) throws DPFTRuntimeException {
		if (notifier == null)
			return;

		String dt = getCurrentTimeStampAsString().substring(0, GlobalConstants.DFPT_DATE_FORMAT.length());
		DPFTNotificationDboSet nset = (DPFTNotificationDboSet) DPFTConnectionFactory
				.initDPFTConnector(getSystemDBConfig()).getDboSet("MAILBOX", "data_dt='" + dt + "'");
		String[] receivers = notifier.split(GlobalConstants.FILE_DELIMETER_COMMA);
		for (String receiver : receivers) {
			DPFTNotificationDbo notif = (DPFTNotificationDbo) nset.add();
			notif.setValue("data_dt", dt);
			notif.setValue("receiver", receiver);
			notif.setValue("mail_title", buildTitleString(title));
			if (msg_body != null && msg_body.length() > 3000) {
				msg_body.substring(0, 3000);
			}
			notif.setValue("mail_content", msg_body);
			DPFTLogger.info(MacroAUTO.class, "dt:" + dt);
			DPFTLogger.info(MacroAUTO.class, "receiver:" + receiver);
			DPFTLogger.info(MacroAUTO.class, "mail_title:" + buildTitleString(title));
			DPFTLogger.info(MacroAUTO.class, "msg_body:" + msg_body);
		}
		nset.save();
		nset.close();
	}

	private static String buildTitleString(String title) {
		return GlobalConstants.MSG_MAIL_TITLE_01 + title;
	}

	public static String getCampaignOwnerEmail(String cmp_code) throws DPFTRuntimeException {
		// UnicaCampaignDboSet cmpSet = (UnicaCampaignDboSet)
		// DPFTConnectionFactory
		// .initDPFTConnector(DPFTUtil.getDBConfig(GlobalConstants.DB_CONN_PROFILE_UNICADBP))
		// .getDboSet("UA_CAMPAIGN");
		// String email = "";
		// if (cmpSet.getCampaign(cmp_code) != null) {
		// email = cmpSet.getCampaign(cmp_code).getOwnerEmail();
		// }
		// cmpSet.close();
		// return email;
		return "";
	}

	public static String getGKSelectTCodeINString(DPFTDboSet set) throws DPFTRuntimeException {
		if (set.isEmpty())
			return "";

		StringBuilder sb = new StringBuilder();
		sb.append("select distinct treatment_code from ").append("D_").append(set.getDbo(0).getString("chal_name"));
		return sb.toString();
	}
}

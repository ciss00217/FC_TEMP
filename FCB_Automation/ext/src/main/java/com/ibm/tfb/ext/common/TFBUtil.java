package com.ibm.tfb.ext.common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDbo;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.dbo.TFBMailGroupDbo;
import com.ibm.tfb.ext.dbo.TFBMailGroupDboSet;
import com.ibm.tfb.ext.dbo.TFBUsageCodeDbo;
import com.ibm.tfb.ext.dbo.TFBUsageCodeDboSet;
import com.ibm.tfb.ext.common.TFBConstants;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDboSet;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class TFBUtil {
	private static final Object lock = new Object();
	private static final Object lock2 = new Object();
	private static final Object lock3 = new Object();
	private static final int MAX_CUST_MAP_SIZE = 1000000;
	private static HashMap<String, String> custMap = new HashMap<String, String>();

	public static String getCustomerSelectINString(DPFTDboSet set, String colname) throws DPFTRuntimeException {
		if (set.isEmpty())
			return "";

		String fk_string = DPFTUtil.getFKQueryString(set.getDbo(0));
		StringBuilder sb = new StringBuilder();
		// sb.append("select customer_id from ").append(set.getDBConnector().getDBConfig().getDBServiceName())
		// 		.append(".dbo.").append("D_").append(set.getDbo(0).getString("chal_name")).append(" where ")
		// 		.append(fk_string);
		sb.append("select customer_id from ")
		.append("D_").append(set.getDbo(0).getString("chal_name")).append(" where ")
		.append(fk_string);
		return sb.toString();
	}

	public static String buildCustomerSelectINString(DPFTDboSet set) throws DPFTRuntimeException {
		if (set.isEmpty())
			return "";

		String fk_string = DPFTUtil.getFKQueryString(set.getDbo(0));
		StringBuilder sb = new StringBuilder();
		sb.append("select customer_id from ").append(set.getDboname()).append(" where ").append(fk_string);
		return sb.toString();
	}

	public static String getDestNameString(DPFTDbo dbo, DPFTDbo id_dbo, String idnu) {
		id_dbo.setValue("treatment_code", dbo.getString("treatment_code"));
		id_dbo.setValue("customer_id", dbo.getString("customer_id"));
		id_dbo.setValue("id_number", idnu);
		return dbo.getString("camp_code") + "||" + dbo.getString("treatment_code") + "||" + idnu;
		// return dbo.getString("camp_code") + "||" +
		// dbo.getString("treatment_code") + "||" +
		// dbo.getString("customer_id");
	}

	public static String getDestNameString(DPFTDbo dbo) {
		return dbo.getString("camp_code") + "||" + dbo.getString("treatment_code") + "||"
				+ dbo.getString("customer_id");
	}

	public static void setSSMHeaderProperties(DPFTDbo new_dbo, DPFTDbo data) {
		setSSMHeaderProperties(new_dbo, data, null, null);
	}

	public static void setSSMHeaderProperties(DPFTDbo new_dbo, DPFTDbo data, DPFTDbo id_dbo, String idnum) {
		new_dbo.setValue("destname",
				(id_dbo != null) ? TFBUtil.getDestNameString(data, id_dbo, idnum) : TFBUtil.getDestNameString(data));
		if (data.getString("isinteractive").equalsIgnoreCase("y")) {
			// new_dbo.setValue("username",
			// DPFTEngine.getSystemProperties("TFB_SSM_I_USRNAME"));
			new_dbo.setValue("password", DPFTEngine.getSystemProperties("TFB_SSM_I_USRPWD"));
			new_dbo.setValue("groupid", DPFTEngine.getSystemProperties("TFB_SSM_I_GROUP_ID"));
		} else {
			// new_dbo.setValue("username",
			// DPFTEngine.getSystemProperties("TFB_SSM_USRNAME"));
			new_dbo.setValue("password", DPFTEngine.getSystemProperties("TFB_SSM_USRPWD"));
			new_dbo.setValue("groupid", DPFTEngine.getSystemProperties("TFB_SSM_GROUP_ID"));
		}
		new_dbo.setValue("priority", DPFTEngine.getSystemProperties("TFB_SSM_PRIORITY"));
		new_dbo.setValue("mid", DPFTEngine.getSystemProperties("TFB_SSM_MID"));
		new_dbo.setValue("objectid", DPFTEngine.getSystemProperties("TFB_SSM_OBJID"));
	}

	public static DPFTConfig getMKTDMConfig() {
		return DPFTUtil.getDBConfig("MKTDM");
	}

	public static DPFTConfig getMKTDMConfig(String MKTDM) {
		return DPFTUtil.getDBConfig(MKTDM);
	}
	
	public static DPFTConfig getMTSFAConfig() {
		return DPFTUtil.getDBConfig("MTSFA");
	}
	
	public static DPFTConfig getSSDOPConfig() {
		return DPFTUtil.getDBConfig("SSDOP");
	}

	public static void setESMHeaderProperties(DPFTDbo new_dbo, DPFTDbo data, DPFTDbo id_dbo, String idnum) {
		new_dbo.setValue("destname", TFBUtil.getDestNameString(data, id_dbo, idnum));
		if (data.getString("isinteractive").equalsIgnoreCase("y")) {
			// new_dbo.setValue("username",
			// DPFTEngine.getSystemProperties("TFB_ESM_I_USRNAME"));
			new_dbo.setValue("password", DPFTEngine.getSystemProperties("TFB_ESM_I_USRPWD"));
			new_dbo.setValue("groupid", DPFTEngine.getSystemProperties("TFB_ESM_I_GROUP_ID"));
		} else {
			// new_dbo.setValue("username",
			// DPFTEngine.getSystemProperties("TFB_ESM_USRNAME"));
			new_dbo.setValue("password", DPFTEngine.getSystemProperties("TFB_ESM_USRPWD"));
			new_dbo.setValue("groupid", DPFTEngine.getSystemProperties("TFB_ESM_GROUP_ID"));
		}
		new_dbo.setValue("priority", DPFTEngine.getSystemProperties("TFB_ESM_PRIORITY"));
		new_dbo.setValue("mid", DPFTEngine.getSystemProperties("TFB_ESM_MID"));
		new_dbo.setValue("objectid", DPFTEngine.getSystemProperties("TFB_ESM_OBJID"));
	}

	public static String getNextActionPlanID() throws DPFTRuntimeException {
		synchronized (lock2) {
			DPFTDboSet seqSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_SEQ", "active=1 and id='" + TFBConstants.WMS_APLN_SEQ_ID + "'");
			seqSet.load();
			if (seqSet.isEmpty()) {
				seqSet.close();
				Object[] params = { TFBConstants.WMS_APLN_SEQ_ID };
				throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0009E", params);
			}
			DPFTDbo seq = seqSet.getDbo(0);
			int current_seq = Integer.valueOf(seq.getString("value"));
			int max_seq = Integer.valueOf(seq.getString("max_value"));
			int st_seq = Integer.valueOf(seq.getString("start_value"));
			String type = seq.getString("type");
			Date sys_date = new Date();
			current_seq++;
			if (current_seq > max_seq) {
				DPFTLogger.info(TFBUtil.class.getName(), "Info: Sequence exceed limit Reset Value...");
				current_seq = st_seq;
			}
			seq.setValue("value", String.valueOf(current_seq));
			seq.setValue("upd_date", sys_date);
			seqSet.save();
			seqSet.close();
			return type + String.format("%08d", current_seq);
		}
	}

	public static String getROCYearMonthString(Date date) {

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(cal.get(Calendar.YEAR) - 1911))
				.append(String.format("%02d", cal.get(Calendar.MONTH) + 1));
		return sb.toString();
	}

	// YYYY-MM-DD to yyyMMdd
	public static String getROCYearMonthDay(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		StringBuilder sb = new StringBuilder();
		sb.append(String.valueOf(cal.get(Calendar.YEAR) - 1911))
				.append(String.format("%02d", cal.get(Calendar.MONTH) + 1))
				.append(String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
		return sb.toString();
	}

	public static void generateObndCtrlRecord(DPFTConnector connector, DPFTDboSet oSet,
			ArrayList<String> cell_code_list, ArrayList<String> cell_name_list, String chal_name, boolean hasHObnd)
			throws DPFTRuntimeException {
		DPFTDbo dbo = oSet.getDbo(0);
		String qString = DPFTUtil.getFKQueryString(dbo);
		if (qString == null) {
			throw new DPFTConnectionException("SYSTEM", "DPFT0011E");
		}
		if (hasHObnd) {
			DPFTOutboundControlDboSet hObndSet = (DPFTOutboundControlDboSet) connector.getDboSet("H_OUTBOUND", qString);
			hObndSet.load();
			if (hObndSet.count() > 0) {
				DPFTLogger.info(TFBUtil.class.getName(), "Records exist in H_OUTBOUND...Delete All Records...");
				hObndSet.deleteAll();
			}
			// Divide outbound table by cell_code
			for (int i = 0; i < cell_code_list.size(); i++) {
				DPFTOutboundControlDbo new_hobnd = (DPFTOutboundControlDbo) hObndSet.add();
				new_hobnd.setBasicInfo(dbo, "O_" + chal_name, chal_name);
				new_hobnd.setValue("cell_code", cell_code_list.get(i));
				new_hobnd.setTotalDataQuantity(oSet, cell_code_list.get(i));
			}
			hObndSet.save();
			hObndSet.close();
		}

		for (int i = 0; i < cell_code_list.size(); i++) {
			int total_exclude_gk = 0, total_exclude = 0, total = 0;
			String cell_code = cell_code_list.get(i);
			String cell_name = cell_name_list.get(i);
			for (int j = 0; j < oSet.count(); j++) {
				if (oSet.getDbo(j).getString("cell_code").equals(cell_code)
						&& oSet.getDbo(j).getString("process_status").equals(GlobalConstants.O_DATA_GK_EXCLUDE)) {
					total_exclude_gk++;
				}
				if (oSet.getDbo(j).getString("cell_code").equals(cell_code)
						&& oSet.getDbo(j).getString("process_status").equals(GlobalConstants.O_DATA_EXCLUDE)) {
					total_exclude++;
				}
				if (oSet.getDbo(j).getString("cell_code").equals(cell_code))
					total++;
			}
			int total_out = total - total_exclude_gk - total_exclude;
			// - replace cell_code to cell_name for all chain
			Object[] params = { dbo.getString("camp_code"), cell_name, String.valueOf(total),
					String.valueOf(total_exclude_gk), String.valueOf(total_exclude), String.valueOf(total_out) };
			// DPFTUtil.pushNotification(
			// DPFTUtil.getCampaignOwnerEmail(dbo.getString("camp_code")),
			// new DPFTMessage("CUSTOM", "TFB00009I", params)
			// );
		}

		DPFTInboundControlDboSet hIbndSet = (DPFTInboundControlDboSet) connector.getDboSet("H_INBOUND", qString);
		hIbndSet.taskComplete();
		hIbndSet.close();
	}

	public static String buildHeaderString(String[] cols, DPFTDbo data, String delimeter) {
		StringBuilder sb = new StringBuilder();
		for (String col : cols) {
			Object value = data.getColumnValue(col);
			if (value != null) {
				if (value instanceof Date) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
					sb.append(sdf.format((Date) value));
				} else {
					sb.append((String) value);
				}
			} else {
				sb.append("");
			}
			sb.append(delimeter);
		}
		return sb.substring(0, sb.length() - delimeter.length());
	}

	public static String buildMultiLineHeader(String[] cols, DPFTDbo data, String delimeter)
			throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		int lineNumber = 1;
		for (String col : cols) {
			Object value = data.getColumnValue(col);
			if (value != null) {
				String valueString = (String) value;
				if (valueString.contains(GlobalConstants.FILE_DELIMETER_COMMA)) {
					String[] stringArray = valueString.split(GlobalConstants.FILE_DELIMETER_COMMA);
					if (lineNumber < stringArray.length) {
						lineNumber = lineNumber + stringArray.length - 1;
					}
				}
			}
		}
		for (int i = 0; i < lineNumber; i++) {
			for (int j = 0; j < cols.length; j++) {
				String col = cols[j];
				Object value = data.getColumnValue(col);
				if (value != null) {
					if (value instanceof Date) {
						SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
						sb.append(sdf.format((Date) value));
					} else {
						String valueString = (String) value;
						if (valueString.contains(GlobalConstants.FILE_DELIMETER_COMMA)) {
							String[] stringArray = valueString.split(GlobalConstants.FILE_DELIMETER_COMMA);
							if (stringArray.length > i) {
								sb.append((String) stringArray[i]);
							} else {
								throw new DPFTConnectionException("SYSTEM", "DPFT0011E");
							}
						} else {
							sb.append((String) value);
						}
					}
				} else {
					sb.append("");
				}
				if (j != cols.length - 1) {
					sb.append(delimeter);
				}
			}
			sb.append(GlobalConstants.FILE_EOL);
		}
		return sb.substring(0, sb.length() - GlobalConstants.FILE_EOL.length());
	}

	public static void processUsageCode(DPFTDboSet oSet, String chal_name) throws DPFTRuntimeException {
		DPFTDbo dbo = oSet.getDbo(0);
		String qString = DPFTUtil.getFKQueryString(dbo);
		if (qString == null) {
			throw new DPFTConnectionException("SYSTEM", "DPFT0011E");
		}

		TFBUsageCodeDboSet oUsgSet = (TFBUsageCodeDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("O_USAGECODE", qString);
		oUsgSet.load();
		if (oUsgSet.count() > 0) {
			DPFTLogger.info(TFBUtil.class.getName(), "Records exist in O_USAGECODE...Delete All Records...");
			oUsgSet.deleteAll();
		}
		for (int i = 0; i < oSet.count(); i++) {
			if (oSet.getDbo(i).isNull("usagecode"))
				continue;
			String[] usageArray = oSet.getDbo(i).getString(("usagecode")).split(";");
			for (String usage : usageArray) {
				TFBUsageCodeDbo new_usgcode = (TFBUsageCodeDbo) oUsgSet.add();
				new_usgcode.setInitialData(oSet.getDbo(i), chal_name, usage.toUpperCase());
			}
		}
		oUsgSet.save();
		oUsgSet.close();
	}

	public static String getNextBranchNo(String cust_id) throws DPFTRuntimeException {
		synchronized (lock3) {
			if (custMap.size() > MAX_CUST_MAP_SIZE)
				custMap.clear();

			String branch_no = custMap.get(cust_id);
			if (branch_no == null) {
				custMap.put(cust_id, "001");
				return "001";
			}
			int current_seq = Integer.valueOf(branch_no);
			current_seq++;
			if (current_seq > 999) {
				DPFTLogger.info(TFBUtil.class.getName(), "Info: Sequence exceed limit Reset Value...");
				current_seq = 1;
			}
			branch_no = String.format("%03d", current_seq);
			custMap.put(cust_id, branch_no);
			return branch_no;
		}
	}

	public static String getSMSContactEmail(String cmp_owner_email, String it_adm_mail) throws DPFTRuntimeException {
		if (cmp_owner_email != null) {
			StringBuilder sb = new StringBuilder();
			sb.append(it_adm_mail).append(GlobalConstants.FILE_DELIMETER_COMMA).append(cmp_owner_email);
			return sb.toString();
		}
		return it_adm_mail;
	}

	public static String getMailGroup(String group_id) throws DPFTRuntimeException {
		TFBMailGroupDboSet gSet = (TFBMailGroupDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("MAILGROUP", "group_name='" + group_id + "'");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < gSet.count(); i++) {
			TFBMailGroupDbo mg = (TFBMailGroupDbo) gSet.getDbo(i);
			sb.append(mg.getEmail()).append(GlobalConstants.FILE_DELIMETER_COMMA);
		}
		gSet.close();
		if (sb.length() > 0)
			return sb.substring(0, sb.length() - 1);
		return null;
	}

	public static String[] generateBarCode(int total_seq) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTDboSet seqSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_SEQ", "active=1 and id='" + TFBConstants.BDM_BARCODE_SEQ_ID + "'");
			seqSet.load();
			if (seqSet.isEmpty()) {
				seqSet.close();
				Object[] params = { TFBConstants.BDM_BARCODE_SEQ_ID };
				throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0009E", params);
			}
			DPFTDbo seq = seqSet.getDbo(0);
			String[] rtnlist = new String[total_seq];
			for (int i = 0; i < total_seq; i++) {
				rtnlist[i] = nextBarCode(seq);
			}
			seqSet.save();
			seqSet.close();
			return rtnlist;
		}
	}

	private static String nextBarCode(DPFTDbo seq) throws DPFTRuntimeException {
		int current_seq = Integer.valueOf(seq.getString("value"));
		int max_seq = Integer.valueOf(seq.getString("max_value"));
		int st_seq = Integer.valueOf(seq.getString("start_value"));
		String type = seq.getString("type");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		Date upd_date = seq.getDate("upd_date");
		Date sys_date = new Date();
		if (!sdf.format(upd_date).equals(sdf.format(sys_date))
				&& seq.getString("is_daily_reset").equalsIgnoreCase("y")) {
			/* reset sequence */
			seq.setValue("value", String.valueOf(st_seq));
			seq.setValue("type", TFBConstants.BDM_BARCODE_SEQ_TYPE_S);
			seq.setValue("upd_date", sys_date);
			current_seq = st_seq;
			type = TFBConstants.BDM_BARCODE_SEQ_TYPE_S;
		} else {
			current_seq++;
			if (current_seq > max_seq && type.equals(TFBConstants.BDM_BARCODE_SEQ_TYPE_S)) {
				/* reset sequence change type to P */
				seq.setValue("value", String.valueOf(st_seq));
				seq.setValue("type", TFBConstants.BDM_BARCODE_SEQ_TYPE_R);
				seq.setValue("upd_date", sys_date);
				type = TFBConstants.BDM_BARCODE_SEQ_TYPE_R;
				current_seq = st_seq;
			} else {
				if (current_seq > max_seq) {
					Object[] params = { TFBConstants.BDM_BARCODE_SEQ_ID, max_seq };
					throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0010E", params);
				}
				seq.setValue("value", String.valueOf(current_seq));
				seq.setValue("upd_date", sys_date);
			}
		}
		return type + String.format("%06d", current_seq);
	}

	public static String[] generateSEQ(String seq_id, int size) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTDboSet seqSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
					.getDboSet("DPFT_SEQ", "active=1 and id='" + seq_id + "'");
			seqSet.load();
			if (seqSet.isEmpty()) {
				seqSet.close();
				Object[] params = { seq_id };
				throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0009E", params);
			}
			DPFTDbo seq = seqSet.getDbo(0);
			String[] rtnlist = new String[size];
			for (int i = 0; i < size; i++) {
				rtnlist[i] = nextSEQ(seq);
			}
			seqSet.save();
			seqSet.close();
			return rtnlist;
		}
	}

	private static String nextSEQ(DPFTDbo seq) {
		int current_seq = Integer.valueOf(seq.getString("value"));
		int max_seq = Integer.valueOf(seq.getString("max_value"));
		int st_seq = Integer.valueOf(seq.getString("start_value"));
		//String type = seq.getString("type");
		Date sys_date = new Date();
		current_seq++;
		if (current_seq > max_seq) {
			DPFTLogger.info(TFBUtil.class.getName(), "Info: Sequence exceed limit Reset Value...");
			current_seq = st_seq;
		}
		seq.setValue("value", String.valueOf(current_seq));
		seq.setValue("upd_date", sys_date);
		//return type + String.format("%08d", current_seq);
		return String.format("%011d", current_seq);
	}

	public static void generateLZIbndCtrlRecord(String target_ds, String data_datetime, String chal_name)
			throws DPFTRuntimeException {
		DPFTInboundControlDboSet cSet = (DPFTInboundControlDboSet) DPFTConnectionFactory
				.initDPFTConnector(DPFTUtil.getSystemDBConfig())
				.getDboSet("H_INBOUND", "timestamp='" + data_datetime + "' and chal_name='" + chal_name + "'");
		DPFTInboundControlDbo ctrl = (DPFTInboundControlDbo) cSet.add();
		ctrl.setValue("timestamp", data_datetime);
		ctrl.setValue("target_ds", target_ds);
		ctrl.setValue("chal_name", chal_name);
		ctrl.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_INIT);
		ctrl.setValue("gk_flg", "Y");
		ctrl.setValue("insta", "1");
		cSet.save();
		cSet.close();
	}

	public static String buildLZQueryString(String timestamp, String chal_name) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		sb.append("camp_code is null and ").append("timestamp='").append(timestamp).append("' and ")
				.append("chal_name='").append(chal_name).append("'");
		return sb.toString();
	}

	public static void generateObndCtrlForLZRecord(DPFTConnector connector, String chal_name, String timestamp,
			String target_tbl, int quantity) throws DPFTRuntimeException {
		/* Set Query criteria for target Table */
		String qString = TFBUtil.buildLZQueryString(timestamp, chal_name);

		DPFTOutboundControlDboSet hObndSet = (DPFTOutboundControlDboSet) connector.getDboSet("H_OUTBOUND", qString);
		hObndSet.load();
		if (hObndSet.count() > 0) {
			DPFTLogger.info(TFBUtil.class.getName(), "Records exist in H_OUTBOUND...Delete All Records...");
			hObndSet.deleteAll();
		}

		DPFTOutboundControlDbo new_hobnd = (DPFTOutboundControlDbo) hObndSet.add();
		new_hobnd.setValue("timestamp", timestamp);
		new_hobnd.setValue("cell_code", timestamp.substring(GlobalConstants.DFPT_DATE_FORMAT.length()));
		new_hobnd.setValue("chal_name", chal_name);
		new_hobnd.setValue("target_ds", target_tbl);
		new_hobnd.setValue("quantity", String.valueOf(quantity));
		new_hobnd.setValue("del_quantity", "0");
		new_hobnd.setValue("insta", "1");
		hObndSet.save();
		hObndSet.close();

		DPFTInboundControlDboSet hIbndSet = (DPFTInboundControlDboSet) connector.getDboSet("H_INBOUND", qString);
		hIbndSet.taskComplete();
		hIbndSet.close();
	}

}

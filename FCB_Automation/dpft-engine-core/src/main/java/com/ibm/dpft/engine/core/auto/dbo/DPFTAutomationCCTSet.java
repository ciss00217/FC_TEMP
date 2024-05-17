package com.ibm.dpft.engine.core.auto.dbo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTAutomationException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTAutomationCCTSet extends DPFTDboSet {
	private final static Pattern cmp_code_pattern = Pattern.compile("C\\d{9}");

	public DPFTAutomationCCTSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);

	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTAutomationCCT(dboname, d, this);
	}

	public int isAllFlowChartFinished(String flowchart_type) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		ArrayList<Integer> notif_ary = new ArrayList<Integer>();
		for (int i = 0; i < count(); i++) {
			if (!this.getDbo(i).getString("flowcharttype").equalsIgnoreCase(flowchart_type))
				continue;
			if (this.getDbo(i).isNull("cct_status"))
				continue;

			if (flowchart_type.equalsIgnoreCase(GlobalConstants.DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_G)) {
				if (this.getDbo(i).getString("cct_status")
						.indexOf(GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_FAILED) != -1) {
					Object[] params = { this.getDbo(i).getString("name"), this.getDbo(i).getString("cct_status") };
					throw new DPFTAutomationException("SYSTEM", "AUTO0009E", params);
				} else if (!this.getDbo(i).getString("cct_status")
						.equals(GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_SUCCESS)) {
					return GlobalConstants.DPFT_AUTOMATION_PS_RC_WAITING;
				}
				sb.append("flowchart = ").append(this.getDbo(i).getString("name")).append(", status = ")
						.append(this.getDbo(i).getString("cct_status")).append(GlobalConstants.FILE_EOL);
			} else {
				if (this.getDbo(i).getString("cct_status").equals(GlobalConstants.DPFT_AUTOMATION_FLOWCHAR_RUN_INPRG)) {
					return GlobalConstants.DPFT_AUTOMATION_PS_RC_WAITING;
				}

				if (flowchart_type.equalsIgnoreCase(GlobalConstants.DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_R)) {
					sb.append("flowchart = ").append(this.getDbo(i).getString("name")).append(", status = ")
							.append(this.getDbo(i).getString("cct_status")).append(GlobalConstants.FILE_EOL);
				} else {
					// Keep finished flowchart index
					notif_ary.add(i);
				}
			}
		}
		for (int i = 0; i < notif_ary.size(); i++) {
			// Notify Campaign Owner
			int idx = notif_ary.get(i);
			StringBuilder sb1 = new StringBuilder();
			sb1.append("flowchart = ").append(this.getDbo(idx).getString("name")).append(", status = ")
					.append(this.getDbo(idx).getString("cct_status")).append(GlobalConstants.FILE_EOL);
			Object[] p1 = { sb1.toString() };
			// DPFTUtil.pushNotification(
			// DPFTUtil.getCampaignOwnerEmail(parseCampaignCode(flowchart_type,
			// this.getDbo(idx).getString("name"))),
			// new DPFTMessage("SYSTEM", "DPFT0044I", p1)
			// );
			sb.append(sb1.toString());
		}
		Object[] params = { sb.toString() };
		// DPFTUtil.pushNotification(new DPFTMessage("SYSTEM", "DPFT0044I",
		// params));
		return GlobalConstants.DPFT_AUTOMATION_PS_RC_TRUE;
	}

	public String parseCampaignCode(String type, String name) {
		int iter = 0;
		if (type.equalsIgnoreCase(GlobalConstants.DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_C)) {
			iter = 1;
		} else if (type.equalsIgnoreCase(GlobalConstants.DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_RP)) {
			iter = 2;
		}
		Matcher m = cmp_code_pattern.matcher(name);
		int i = 0;
		String cmp_code = null;
		while (m.find() && i < iter) {
			cmp_code = m.group(0);
			i++;
		}
		return cmp_code;
	}

	public String[] getRunnableFlowChartByType(String type) throws DPFTRuntimeException {
		if (type.equalsIgnoreCase(GlobalConstants.DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_G)
				|| type.equalsIgnoreCase(GlobalConstants.DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_R)) {
			Pattern p = Pattern.compile(type + "_\\d{2}_\\w{2}$");
			HashMap<String, String> map = new HashMap<String, String>();
			HashMap<String, Integer> groupMap = new HashMap<String, Integer>();
			for (int i = 0; i < count(); i++) {
				String name = this.getDbo(i).getString("name");
				Matcher m = p.matcher(name);
				if (m.find()) {
					String gno = m.group(0);
					// ex:G_02_HH
					String groupString = gno.substring(gno.indexOf("_") + 4);
					if (groupMap.get(groupString) != null) {
						groupMap.put(groupString, groupMap.get(groupString) + 1);
					} else {
						groupMap.put(groupString, 1);
					}
					map.put(gno.substring(gno.indexOf("_") + 1), this.getDbo(i).getString("filename"));
				}
			}
			String[] flist = new String[map.size()];
			int startIndex = 0;
			for (String s : groupMap.keySet()) {
				int i = groupMap.get(s);
				for (String m : map.keySet()) {
					if (m.substring(m.indexOf("_") + 1).equals(s)) {
						int index = startIndex + Integer.valueOf(m.substring(0, 2)) - 1;
						flist[index] = map.get(m);
					}
				}
				startIndex = startIndex + i;
			}
			return flist;
		} else {
			ArrayList<String> filelist = new ArrayList<String>();
			ArrayList<Integer> idx = new ArrayList<Integer>();
			for (int i = 0; i < count(); i++) {
				if (this.getDbo(i).getString("flowcharttype").equalsIgnoreCase(type)) {
					filelist.add(this.getDbo(i).getString("filename"));
					idx.add(i);
				}
			}
			return filelist.toArray(new String[filelist.size()]);
		}
	}
}

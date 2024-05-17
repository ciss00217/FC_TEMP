package com.ibm.tfb.ext.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTPrioritySettingDbo;
import com.ibm.dpft.engine.core.dbo.DPFTPrioritySettingDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBConstants;
import com.ibm.tfb.ext.common.TFBUtil;

public class MKTCustContAddOnline extends DPFTDboSet {

	public MKTCustContAddOnline(DPFTConfig db_cfg, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(db_cfg, selectAttrs, tbname, whereclause);
	}

	public static MKTCustContAddOnline newInst(String whereclause) throws DPFTRuntimeException {
		DPFTConfig db_cfg = DPFTUtil.getSystemDBConfig();
		String tbname = " mkt_cust_cont_AddOnline ";
		String selectAttrs = " CUST_ID, BIZ_CAT, CONT_CD, CONT_INFO ";
		MKTCustContAddOnline inst = new MKTCustContAddOnline(db_cfg, selectAttrs, tbname, whereclause);
		inst.doCache();
		return inst;
	}

	public String getPrioritizedEmail(String cust_id, String template, String p_code) throws DPFTRuntimeException {
		return getPrioritizedMobilePhone(cust_id, template, p_code, TFBConstants.MKTDM_CONT_CD_EMAIL, 1);
	}
	
	public String getPrioritizedMobilePhone(String cust_id, String template, String p_code) throws DPFTRuntimeException {
		return getPrioritizedMobilePhone(cust_id, template, p_code, TFBConstants.MKTDM_CONT_CD_MOBILE, -1);
	}
	
	public String getPrioritized() {
		return prioritized;
	}
	
	String prioritized = "";
	
	private String getPrioritizedMobilePhone(String cust_id, String template, String p_code, String contCd, int stop) throws DPFTRuntimeException {
		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		DPFTLogger.debug(this, "stop=" + stop);
		prioritized = "";
		String[] p = pr.getPrioritySettings();
		String contInfo = getBizTypeNoRefresh(cust_id, p[0], contCd);
		prioritized = p[0];
		for (int i = 1; i < p.length; i++) {
			if (contInfo == null) {
				contInfo = getBizTypeNoRefresh(cust_id, p[i], contCd);
				prioritized = p[i];
			}
			if (contInfo != null) {
				break;
			}
			if (i == stop) {
				break;
			}
		}
		return contInfo;
	}
	// For CTI Channel
	public String[] getAllPrioritizedEmail(String cust_id, String template, String p_code) throws DPFTRuntimeException {
		String[] email = new String[3];
		//setEmail_Src("");

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}

		String[] p = pr.getPrioritySettings();
		email[0] = getBizTypeNoRefresh(cust_id.trim(), p[0], TFBConstants.MKTDM_CONT_CD_EMAIL);
		email[1] = getBizTypeNoRefresh(cust_id.trim(), p[1], TFBConstants.MKTDM_CONT_CD_EMAIL);
		//email[2] = getEmailByBizType(cust_id.trim(), p[2]);
		
		return email;
	}
	
	// For CTI Channel
	public String[] getAllPrioritizedMobilePhone(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String[] mobile = new String[3];

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		mobile[0] = getBizTypeNoRefresh(cust_id, p[0], TFBConstants.MKTDM_CONT_CD_MOBILE);
		mobile[1] = getBizTypeNoRefresh(cust_id, p[1], TFBConstants.MKTDM_CONT_CD_MOBILE);
		//mobile[2] = getMobileByBizType(cust_id, p[2]);
		return mobile;
	}
	
	private String getBizTypeNoRefresh(String cust_id, String bizCat, String contCd) throws DPFTRuntimeException {
		String key = cust_id + bizCat + contCd;
		if (cacheMap != null && cacheMap.containsKey(key)) {
			return cacheMap.get(key);
		}
		reset(" CUST_ID='" + cust_id + "' AND BIZ_CAT='" + bizCat + "' AND CONT_CD='" + contCd + "'");
		load();
		String contInfo = null;
		try {
			contInfo = getDbo(0).getString("CONT_INFO");
		} catch(Exception e) {
		}
		DPFTLogger.debug(this,
				"CUST_ID=" + cust_id + ",BIZ_CAT:" + bizCat + ",CONT_CD=" + contCd + ",CONT_INFO=" + contInfo);
		return contInfo;
	}
	
	private HashMap<String, String> cacheMap = new HashMap<>();
	
	private void doCache() throws DPFTRuntimeException {
		load();
		for (int i = 0; i < count(); i++) {
			String CUST_ID = getDbo(i).getString("CUST_ID");
			String BIZ_CAT = getDbo(i).getString("BIZ_CAT");
			String CONT_CD = getDbo(i).getString("CONT_CD");
			String contInfo = getDbo(i).getString("CONT_INFO");
			String key = CUST_ID + BIZ_CAT + CONT_CD;
			cacheMap.put(key, contInfo);
		}
		clear();
	}
	
	@Override
	public void close() throws DPFTRuntimeException {
		super.close();
		cacheMap = new HashMap<>();
	}
}

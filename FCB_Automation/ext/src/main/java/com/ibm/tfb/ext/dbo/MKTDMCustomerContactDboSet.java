package com.ibm.tfb.ext.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTPrioritySettingDbo;
import com.ibm.dpft.engine.core.dbo.DPFTPrioritySettingDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.tfb.ext.common.TFBConstants;

public class MKTDMCustomerContactDboSet extends DPFTDboSet {
	public MKTDMCustomerContactDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	private HashMap<String, String> vMap = null;
	private HashMap<String, String> nameMap = null;
	private HashMap<String, String> bdayMap = null;
	private HashMap<String, String> decryptedIDMap = null;
	private String CONT_INFO = "";
	private String EMAIL_SRC = "";

	@Override
	public void load() throws DPFTRuntimeException {
		super.load();
		if (vMap == null)
			vMap = new HashMap<String, String>();
		if (nameMap == null)
			nameMap = new HashMap<String, String>();
		if (bdayMap == null)
			bdayMap = new HashMap<String, String>();
		if (decryptedIDMap == null)
			decryptedIDMap = new HashMap<String, String>();
		vMap.clear();
		nameMap.clear();
		bdayMap.clear();
		decryptedIDMap.clear();
		if (!("").equals(CONT_INFO)) {
			// DPFTLogger.info(this, "count():" + count());
			for (int i = 0; i < count(); i++) {
				// String key = this.getDbo(i).getString("cust_id") +
				// this.getDbo(i).getString("cont_cd") +
				// this.getDbo(i).getString("biz_cat");
				//DPFTLogger.info(this, "THERE THERE THERE CUSTOMER_ID : " + this.getDbo(i).getString("cust_id"));
				//DPFTLogger.info(this, "THERE THERE THERE D_CUSTOMER_ID : " + this.getDbo(i).getString("D_CUST_ID"));
				//DPFTLogger.info(this, "THERE THERE THERE NAME : " + this.getDbo(i).getString("cust_NAME"));
				//DPFTLogger.info(this, "THERE THERE THERE BIRTHDAY : " + this.getDbo(i).getString("BIRTH_DATE"));
				//DPFTLogger.info(this, "THERE THERE THERE BIZ_CAT : " + this.getDbo(i).getString("biz_cat"));
				//DPFTLogger.info(this, "THERE THERE THERE CONT_CD : " + this.getDbo(i).getString("cont_cd"));
				//DPFTLogger.info(this, "THERE THERE THERE CONT_INFO : " + this.getDbo(i).getString("cont_info"));
				//DPFTLogger.info(this, "count(): " + count());
				String key = this.getDbo(i).getString("cust_id").trim() + this.getDbo(i).getString("biz_cat")
						+ this.getDbo(i).getString("cont_cd");
				vMap.put(key, this.getDbo(i).getString("cont_info"));
				if (!nameMap.containsKey(this.getDbo(i).getString("cust_id").trim())) {
					nameMap.put(this.getDbo(i).getString("cust_id").trim(),
							this.getDbo(i).getString("CUST_NAME"));
					DPFTLogger.debug(this, this.getDbo(i).getString("cust_id").trim() + ": " + this.getDbo(i).getString("CUST_NAME"));
				}
				if (!bdayMap.containsKey(this.getDbo(i).getString("cust_id").trim())) {
					bdayMap.put(this.getDbo(i).getString("cust_id").trim(),
							this.getDbo(i).getString("BIRTH_DATE"));
					DPFTLogger.debug(this, this.getDbo(i).getString("cust_id").trim() + ": " + this.getDbo(i).getString("BIRTH_DATE"));
				}
				if (!decryptedIDMap.containsKey(this.getDbo(i).getString("cust_id").trim())) {
					decryptedIDMap.put(this.getDbo(i).getString("cust_id").trim(),
							this.getDbo(i).getString("D_CUST_ID"));
					DPFTLogger.debug(this, this.getDbo(i).getString("cust_id").trim() + ": " + this.getDbo(i).getString("D_CUST_ID"));
				}
				//DPFTLogger.info(this, "vMap key:" + key);
				//DPFTLogger.info(this, "vMap value:" + this.getDbo(i).getString("cont_info"));
			}
			DPFTLogger.debug(this, "vMap done");
		}
	}
	
	public void loadAll() throws DPFTRuntimeException {
		super.load();
		if (vMap == null)
			vMap = new HashMap<String, String>();
		if (nameMap == null)
			nameMap = new HashMap<String, String>();
		if (bdayMap == null)
			bdayMap = new HashMap<String, String>();
		if (decryptedIDMap == null)
			decryptedIDMap = new HashMap<String, String>();
		vMap.clear();
		nameMap.clear();
		bdayMap.clear();
		decryptedIDMap.clear();
		for (int i = 0; i < count(); i++) {
			String key = this.getDbo(i).getString("cust_id").trim() + this.getDbo(i).getString("biz_cat")
					+ this.getDbo(i).getString("cont_cd");
			vMap.put(key, this.getDbo(i).getString("cont_info"));
			if (!nameMap.containsKey(this.getDbo(i).getString("cust_id").trim())) {
				nameMap.put(this.getDbo(i).getString("cust_id").trim(),
						this.getDbo(i).getString("CUST_NAME"));
				DPFTLogger.debug(this, this.getDbo(i).getString("cust_id").trim() + ": " + this.getDbo(i).getString("CUST_NAME"));
			}
			if (!bdayMap.containsKey(this.getDbo(i).getString("cust_id").trim())) {
				bdayMap.put(this.getDbo(i).getString("cust_id").trim(),
						this.getDbo(i).getString("BIRTH_DATE"));
				DPFTLogger.debug(this, this.getDbo(i).getString("cust_id").trim() + ": " + this.getDbo(i).getString("BIRTH_DATE"));
			}
			if (!decryptedIDMap.containsKey(this.getDbo(i).getString("cust_id").trim())) {
				decryptedIDMap.put(this.getDbo(i).getString("cust_id").trim(),
						this.getDbo(i).getString("D_CUST_ID"));
				DPFTLogger.debug(this, this.getDbo(i).getString("cust_id").trim() + ": " + this.getDbo(i).getString("D_CUST_ID"));
			}
		}
		DPFTLogger.debug(this, "vMap done");
	}

	public void loadByCont(String cont) throws DPFTRuntimeException {
		super.load();
		if (vMap == null)
			vMap = new HashMap<String, String>();
		vMap.clear();

		// DPFTLogger.info(this, "count():" + count());
		for (int i = 0; i < count(); i++) {
			// String key = this.getDbo(i).getString("cust_id") +
			// this.getDbo(i).getString("cont_cd") +
			// this.getDbo(i).getString("biz_cat");
			String key = this.getDbo(i).getString("CUSTOMER_ID").trim();
			// DPFTLogger.info(this, "vMap key:" + key);
			// DPFTLogger.info(this, "vMap value:" +
			// this.getDbo(i).getString(CONT_INFO));
			vMap.put(key, this.getDbo(i).getString(cont));
		}

	}

	@Override
	public void close() throws DPFTRuntimeException {
		super.close();
		vMap.clear();
		this.clear();
	}

	public String getCONT_INFO() {
		return CONT_INFO;
	}

	public void setCONT_INFO(String cONT_INFO) {
		CONT_INFO = cONT_INFO;
	}
	
	public String getEmail_Src() {
		return EMAIL_SRC;
	}
	
	public void setEmail_Src (String eMAIL_SRC) {
		EMAIL_SRC = eMAIL_SRC;
	}

	public String getContactInfoByField(String infoField, String cust_id) throws DPFTRuntimeException {
		if (!CONT_INFO.equals(infoField)) {
			setCONT_INFO(infoField);
			load();
		}
		return getContactInfo(cust_id, infoField);
	}

	public String getContactInfoByCont(String infoField, String cust_id) throws DPFTRuntimeException {
		if (!CONT_INFO.equals(infoField)) {
			setCONT_INFO(infoField);
			loadByCont(infoField);
		}
		return getContactInfo(cust_id, infoField);
	}

	public String getContactInfoByField(String infoField, String cust_id, String biz_type) throws DPFTRuntimeException {
		if (!CONT_INFO.equals(infoField)) {

			setCONT_INFO(infoField);
			load();
		}
		return getContactInfo(cust_id, infoField, biz_type);
	}

	public String getEmail(String cust_id) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_EMAIL);
	}

	public String getMobile(String cust_id) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_MOBILE_1);
	}

	public String getName(String cust_id) throws DPFTRuntimeException {
		return nameMap.get(cust_id);
	}
	
	public String getBDay(String cust_id) throws DPFTRuntimeException {
		return bdayMap.get(cust_id);
	}
	
	public String getDecryptedID(String cust_id) throws DPFTRuntimeException {
		return decryptedIDMap.get(cust_id);
	}

	public String getContactInfo(String cust_id) throws DPFTRuntimeException {
		String key = cust_id;
		return vMap.get(key);
	}

	private String getContactInfo(String cust_id, String cont_cd) throws DPFTRuntimeException {

		return getContactInfo(cust_id, cont_cd, TFBConstants.MKTDM_CONT_BIZTYPE_BNK);
		// return getContactInfo(cust_id);
	}

	private String getContactInfo(String cust_id, String cont_cd, String biz_type) throws DPFTRuntimeException {
		// for (int i = 0; i < this.count(); i++) {
		// MKTDMCustomerContactDbo dbo = (MKTDMCustomerContactDbo)
		// this.getDbo(i);
		// if (biz_type != null) {
		// if (dbo.find(cust_id, cont_cd, biz_type)) {
		// return dbo.getString(cont_cd);
		// }
		// } else {
		// if (dbo.find(cust_id, cont_cd)) {
		// return dbo.getString(cont_cd);
		// }
		// }
		// }
		// String key = cust_id + cont_cd + biz_type;
		String key = cust_id + biz_type + cont_cd;
		if (!CONT_INFO.equals(cont_cd)) {
//			DPFTLogger.info(this, "cont_cd:" + cont_cd);
			setCONT_INFO(cont_cd);
			load();
		}
		//DPFTLogger.info(this, "Query vMap key: " + key);
		return vMap.get(key);
	}

	private String getContactInfoNoRefresh(String cust_id, String cont_cd, String biz_type) throws DPFTRuntimeException {
		String key = cust_id + biz_type + cont_cd;
		return vMap.get(key);
	}
	
	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new MKTDMCustomerContactDbo(dboname, d, this);
	}

	public String getChnNameByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_CHN_NAME, biz_type);
	}

	public String getAddrByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_ADDR_COMM, biz_type);
	}

	public String getZipCodeByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_ZIPCD_COMM, biz_type);
	}

	public String getResidentAddrCodeByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_RES_ADDR, biz_type);
	}
	
	public String getResidentAddrCodeByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_RES_ADDR, biz_type);
	}

	public String getResidentZipCodeByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_RES_ZIP, biz_type);
	}
	
	public String getResidentZipCodeByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_RES_ZIP, biz_type);
	}
	
	public String getHouseAddrCodeByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_ADDR, biz_type);
	}
	
	public String getHouseAddrCodeByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_ADDR, biz_type);
	}

	public String getHouseZipCodeByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_ZIP, biz_type);
	}
	
	public String getHouseZipCodeByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_ZIP, biz_type);
	}

	public String getAddrCodeByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_ADDR, biz_type);
	}

	/*
	 * add house address option start house address zipcode
	 */
	public String getAddrByBizTypeH(String cust_id, String biz_type) throws DPFTRuntimeException {

		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_COMM, biz_type);
	}

	public String getZipCodeByBizTypeH(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_HOUZIP_COMM, biz_type);
	}
	/*
	 * end
	 */

	public String getEmailByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {

		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_EMAIL, biz_type);
	}
	
	public String getEmailByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {

		return getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_EMAIL, biz_type);
	}

	public String getDayTelByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_DAY, biz_type);
	}

	public String getNightTelByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_NIGHT, biz_type);
	}

	public String getOfficeTelAreaByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_OFF_ARE, biz_type);
	}

	public String getOfficeTelByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_OFF, biz_type);
	}

	public String getOfficeExtTelByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_OFF_EXT, biz_type);
	}

	public String getCommTelAreByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_COM_TEL_ARE, biz_type);
	}

	public String getCommTelByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		return getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_COM_TEL, biz_type);
	}

	public String getMobileByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
//		String mobile = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_MOBILE_1, biz_type);
//		if (mobile == null)
//			mobile = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_MOBILE_2, biz_type);
		String mobile = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_MOBILE, biz_type);
		return mobile;
	}
	
	public String getMobileByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		String mobile = getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_MOBILE, biz_type);
		return mobile;
	}
	
	public String getHomePhoneByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		String phone = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_TEL, biz_type);
		return phone;
	}
	
	public String getHomePhoneByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		String phone = getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_HOU_TEL, biz_type);
		return phone;
	}
	
	public String getCompanyPhoneByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {
		String phone = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_COM_TEL, biz_type);
		return phone;
	}
	
	public String getCompanyPhoneByBizTypeNoRefresh(String cust_id, String biz_type) throws DPFTRuntimeException {
		String phone = getContactInfoNoRefresh(cust_id, TFBConstants.MKTDM_CONT_CD_COM_TEL, biz_type);
		return phone;
	}

	public String getFieldByBizType(String cust_id, String biz_type, String field) throws DPFTRuntimeException {
		String getField = getContactInfo(cust_id, field, biz_type);
		return getField;
	}

	public String[] getPrioritizedAddr(String cust_id, String template, String p_code) throws DPFTRuntimeException {
		String[] addr_info = new String[3];

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		if (p_code.equals("04") || p_code.equals("05") || p_code.equals("06")) {
			String[] p = pr.getPrioritySettings();
			addr_info[0] = getAddrByBizTypeH(cust_id, p[0]);
			addr_info[1] = getZipCodeByBizTypeH(cust_id, p[0]);
			if (addr_info[0] == null || addr_info[2] == null) {
				addr_info[0] = getAddrByBizType(cust_id, p[1]);
				addr_info[1] = getZipCodeByBizType(cust_id, p[1]);
				addr_info[2] = getAddrCodeByBizType(cust_id, p[1]);
				if (addr_info[0] == null || addr_info[2] == null) {
					addr_info[0] = getAddrByBizType(cust_id, p[2]);
					addr_info[1] = getZipCodeByBizType(cust_id, p[2]);
					addr_info[2] = getAddrCodeByBizType(cust_id, p[2]);
				}
			}
		} else {
			String[] p = pr.getPrioritySettings();
			addr_info[0] = getAddrByBizType(cust_id, p[0]);
			addr_info[1] = getZipCodeByBizType(cust_id, p[0]);
			addr_info[2] = getAddrCodeByBizType(cust_id, p[0]);
			if (addr_info[0] == null || addr_info[2] == null) {
				addr_info[0] = getAddrByBizType(cust_id, p[1]);
				addr_info[1] = getZipCodeByBizType(cust_id, p[1]);
				addr_info[2] = getAddrCodeByBizType(cust_id, p[1]);
				if (addr_info[0] == null || addr_info[2] == null) {
					addr_info[0] = getAddrByBizType(cust_id, p[2]);
					addr_info[1] = getZipCodeByBizType(cust_id, p[2]);
					addr_info[2] = getAddrCodeByBizType(cust_id, p[2]);
				}
			}
		}
		return addr_info;
	}

	public String[] getPrioritizedResidentAddrWithoutAddrCode(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String[] addr_info = new String[2];

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		addr_info[0] = getResidentAddrCodeByBizType(cust_id, p[0]);
		addr_info[1] = getResidentZipCodeByBizType(cust_id, p[0]);
		if (addr_info[0] == null) {
			addr_info[0] = getResidentAddrCodeByBizType(cust_id, p[1]);
			addr_info[1] = getResidentZipCodeByBizType(cust_id, p[1]);
			if (addr_info[0] == null) {
				addr_info[0] = getResidentAddrCodeByBizType(cust_id, p[2]);
				addr_info[1] = getResidentZipCodeByBizType(cust_id, p[2]);
			}
		}
		return addr_info;
	}
	
	public String[] getPrioritizedResidentAddrWithoutAddrCodeNoRefresh(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String[] addr_info = new String[2];

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		addr_info[0] = getResidentAddrCodeByBizTypeNoRefresh(cust_id, p[0]);
		addr_info[1] = getResidentZipCodeByBizTypeNoRefresh(cust_id, p[0]);
		if (addr_info[0] == null) {
			addr_info[0] = getResidentAddrCodeByBizTypeNoRefresh(cust_id, p[1]);
			addr_info[1] = getResidentZipCodeByBizTypeNoRefresh(cust_id, p[1]);
			if (addr_info[0] == null) {
				addr_info[0] = getResidentAddrCodeByBizTypeNoRefresh(cust_id, p[2]);
				addr_info[1] = getResidentZipCodeByBizTypeNoRefresh(cust_id, p[2]);
			}
		}
		return addr_info;
	}
	
	public String[] getPrioritizedHouseAddrWithoutAddrCode(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String[] addr_info = new String[2];

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		addr_info[0] = getHouseAddrCodeByBizType(cust_id, p[0]);
		addr_info[1] = getHouseZipCodeByBizType(cust_id, p[0]);
		if (addr_info[0] == null) {
			addr_info[0] = getHouseAddrCodeByBizType(cust_id, p[1]);
			addr_info[1] = getHouseZipCodeByBizType(cust_id, p[1]);
			if (addr_info[0] == null) {
				addr_info[0] = getHouseAddrCodeByBizType(cust_id, p[2]);
				addr_info[1] = getHouseZipCodeByBizType(cust_id, p[2]);
			}
		}
		return addr_info;
	}
	
	public String[] getPrioritizedHouseAddrWithoutAddrCodeNoRefresh(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String[] addr_info = new String[2];

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		addr_info[0] = getHouseAddrCodeByBizTypeNoRefresh(cust_id, p[0]);
		addr_info[1] = getHouseZipCodeByBizTypeNoRefresh(cust_id, p[0]);
		if (addr_info[0] == null) {
			addr_info[0] = getHouseAddrCodeByBizTypeNoRefresh(cust_id, p[1]);
			addr_info[1] = getHouseZipCodeByBizTypeNoRefresh(cust_id, p[1]);
			if (addr_info[0] == null) {
				addr_info[0] = getHouseAddrCodeByBizTypeNoRefresh(cust_id, p[2]);
				addr_info[1] = getHouseZipCodeByBizTypeNoRefresh(cust_id, p[2]);
			}
		}
		return addr_info;
	}

	public String getPrioritizedEmail(String cust_id, String template, String p_code) throws DPFTRuntimeException {
		String email = null;
		setEmail_Src("");

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}

		String[] p = pr.getPrioritySettings();
		email = getEmailByBizType(cust_id.trim(), p[0]);
		setEmail_Src(p[0]);
		// DPFTLogger.info(this, "emailp[0]:" + email);
		if (email == null) {
			email = getEmailByBizType(cust_id.trim(), p[1]);
			setEmail_Src(p[1]);
			// DPFTLogger.info(this, "emailp[1]:" + email);
			// if (email == null) {
			// 	email = getEmailByBizType(cust_id.trim(), p[2]);
			// 	setEmail_Src(p[2]);
				// DPFTLogger.info(this, "emailp[2]:" + email);
			// }
		}
		return email;
	}
	
	public String getPrioritizedEmailNoRefresh(String cust_id, String template, String p_code) throws DPFTRuntimeException {
		String email = null;
		setEmail_Src("");

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}

		String[] p = pr.getPrioritySettings();
		email = getEmailByBizTypeNoRefresh(cust_id.trim(), p[0]);
		setEmail_Src(p[0]);
		if (email == null) {
			email = getEmailByBizTypeNoRefresh(cust_id.trim(), p[1]);
			setEmail_Src(p[1]);
		}
		return email;
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
		email[0] = getEmailByBizTypeNoRefresh(cust_id.trim(), p[0]);
		email[1] = getEmailByBizTypeNoRefresh(cust_id.trim(), p[1]);
		//email[2] = getEmailByBizType(cust_id.trim(), p[2]);
		
		return email;
	}

	public String getPrioritizedMobilePhone(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String mobile = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		mobile = getMobileByBizType(cust_id, p[0]);
		if (mobile == null) {
			mobile = getMobileByBizType(cust_id, p[1]);
			if (mobile == null) {
				mobile = getMobileByBizType(cust_id, p[2]);
			}
		}
		return mobile;
	}
	
	public String getPrioritizedMobilePhoneNoRefresh(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String mobile = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		mobile = getMobileByBizTypeNoRefresh(cust_id, p[0]);
		if (mobile == null) {
			mobile = getMobileByBizTypeNoRefresh(cust_id, p[1]);
			if (mobile == null) {
				mobile = getMobileByBizTypeNoRefresh(cust_id, p[2]);
			}
		}
		return mobile;
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
		mobile[0] = getMobileByBizTypeNoRefresh(cust_id, p[0]);
		mobile[1] = getMobileByBizTypeNoRefresh(cust_id, p[1]);
		//mobile[2] = getMobileByBizType(cust_id, p[2]);
		return mobile;
	}
	
	public String getPrioritizedHomePhone(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String phone = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		phone = getHomePhoneByBizType(cust_id, p[0]);
		if (phone == null) {
			phone = getHomePhoneByBizType(cust_id, p[1]);
			if (phone == null) {
				phone = getHomePhoneByBizType(cust_id, p[2]);
			}
		}
		return phone;
	}
	
	public String getPrioritizedHomePhoneNoRefresh(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String phone = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		phone = getHomePhoneByBizTypeNoRefresh(cust_id, p[0]);
		if (phone == null) {
			phone = getHomePhoneByBizTypeNoRefresh(cust_id, p[1]);
			if (phone == null) {
				phone = getHomePhoneByBizTypeNoRefresh(cust_id, p[2]);
			}
		}
		return phone;
	}
	
	public String getPrioritizedCompanyPhone(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String phone = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		phone = getCompanyPhoneByBizType(cust_id, p[0]);
		if (phone == null) {
			phone = getCompanyPhoneByBizType(cust_id, p[1]);
			if (phone == null) {
				phone = getCompanyPhoneByBizType(cust_id, p[2]);
			}
		}
		return phone;
	}
	
	public String getPrioritizedCompanyPhoneNoRefresh(String cust_id, String template, String p_code)
			throws DPFTRuntimeException {
		String phone = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		phone = getCompanyPhoneByBizTypeNoRefresh(cust_id, p[0]);
		if (phone == null) {
			phone = getCompanyPhoneByBizTypeNoRefresh(cust_id, p[1]);
			if (phone == null) {
				phone = getCompanyPhoneByBizTypeNoRefresh(cust_id, p[2]);
			}
		}
		return phone;
	}

	public String getPrioritizedField(String cust_id, String template, String p_code, String field)
			throws DPFTRuntimeException {
		String mobile = null;

		DPFTPrioritySettingDboSet pSet = DPFTEngine.getPriorityCodeSetting();
		DPFTPrioritySettingDbo pr = pSet.getPrioritySetting(template, p_code);
		if (pr == null) {
			Object[] params = { template, p_code };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0007E", params);
		}
		String[] p = pr.getPrioritySettings();
		mobile = getFieldByBizType(cust_id, p[0], field);
		if (mobile == null) {
			mobile = getFieldByBizType(cust_id, p[1], field);
			if (mobile == null) {
				mobile = getFieldByBizType(cust_id, p[2], field);
			}
		}
		return mobile;
	}

	public String getOfficePhoneByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {

		String off_are = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_OFF_ARE, biz_type);
		String off = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_OFF, biz_type);
		String off_ext = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_TEL_OFF_EXT, biz_type);

		if (off_are == null && off_are == null)
			return null;

		return off_are + off + (off_ext == null ? "" : "#" + off_ext);
	}

	public String getCommPhoneByBizType(String cust_id, String biz_type) throws DPFTRuntimeException {

		String comm_are = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_COM_TEL_ARE, biz_type);
		String comm = getContactInfo(cust_id, TFBConstants.MKTDM_CONT_CD_COM_TEL, biz_type);

		if (comm_are == null && comm == null)
			return null;

		return comm_are + comm;
	}

}

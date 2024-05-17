package com.ibm.tfb.ext.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class MKTDMCustomerDboSet extends DPFTDboSet {
	public MKTDMCustomerDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	private HashMap<String, MKTDMCustomerDbo> vMap = null;

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new MKTDMCustomerDbo(dboname, d, this);
	}
	
	@Override
	public void load() throws DPFTRuntimeException {
		super.load();
		if(vMap == null)
			vMap = new HashMap<String, MKTDMCustomerDbo>();
		vMap.clear();
		for(int i = 0; i < count(); i++){
			String key = this.getDbo(i).getString("cust_id");
			vMap.put(key, (MKTDMCustomerDbo) this.getDbo(i));
		}
	}
	
	@Override
	public void close() throws DPFTRuntimeException {
		super.close();
		vMap.clear();
		this.clear();
	}
	
	private String getCustInfo(String cust_id, String colname) throws DPFTRuntimeException {
		
//		for(int i = 0; i < this.count(); i++){
//			MKTDMCustomerDbo dbo = (MKTDMCustomerDbo) this.getDbo(i);
//			if(dbo.find(cust_id)){
//				return dbo.getString(colname);
//			}
//		}
		return vMap.get(cust_id).getString(colname);
	}

	public String getCustName(String cust_id) throws DPFTRuntimeException {
		
		return getCustInfo(cust_id, "CUST_NM_CUST");
	}

	public String getSysNo(String cust_id) throws DPFTRuntimeException {
		
		return getCustInfo(cust_id, "INSSEQ");
	}

	public String getGender(String cust_id) throws DPFTRuntimeException {
		String cust_info = getCustInfo(cust_id, "GNDR_CUST");
		if(cust_info == null){
			return null;
		}
		
		if(cust_info.equals("1") || cust_info.equals("M"))
			return DPFTEngine.getSystemProperties("tfb.gndr.male");
		else if(cust_info.equals("2") || cust_info.equals("F"))
			return DPFTEngine.getSystemProperties("tfb.gndr.female");
		return null;
//		Object[] params = {cust_info};
//		throw new TFBInvalidCustInfoException("CUSTOM", "TFB00002E", params);
	}

	public String getDateAsString(String cust_id, String colname) throws DPFTRuntimeException {
//		for(int i = 0; i < this.count(); i++){
//			MKTDMCustomerDbo dbo = (MKTDMCustomerDbo) this.getDbo(i);
//			if(dbo.find(cust_id)){
//				return DPFTUtil.convertDateObject2DateString(dbo.getDate(colname), GlobalConstants.DFPT_DATE_FORMAT);
//			}
//		}
		return DPFTUtil.convertDateObject2DateString(vMap.get(cust_id).getDate(colname), GlobalConstants.DFPT_DATE_FORMAT);
	}
}

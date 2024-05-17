package com.ibm.dpft.engine.core.dbo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTDboException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class ResFileDataLayoutDetailDboSet extends DPFTDboSet {

	
	public ResFileDataLayoutDetailDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new ResFileDataLayoutDetailDbo(dboname, d, this);
	}

	public String[] getColumnsInOrder() throws NumberFormatException, DPFTRuntimeException {
		int cnt = count();
		String[] rtnV = new String[cnt];
		for(int i = 0; i < cnt; i++){
			int index = Integer.valueOf(this.getDbo(i).getString("col_order"));
			index--;
			rtnV[index] = this.getDbo(i).getString("f_col");
		}
		return rtnV;
	}

	public HashMap<String, String> getColumnsMapping() throws DPFTRuntimeException {
		int cnt = count();
		HashMap<String, String> rtnMap = new HashMap<String, String>();
		for(int i = 0; i < cnt; i++){
			rtnMap.put(this.getDbo(i).getString("f_col"), this.getDbo(i).getString("target_col"));
		}
		return rtnMap;
	}

	public HashMap<String, Integer> getColumnsLengthMapping() throws NumberFormatException, DPFTRuntimeException {
		int cnt = count();
		HashMap<String, Integer> rtnMap = new HashMap<String, Integer>();
		for(int i = 0; i < cnt; i++){
			rtnMap.put(this.getDbo(i).getString("f_col"), Integer.valueOf(this.getDbo(i).getString("f_col_len")));
		}
		return rtnMap;
	}

	public boolean isDate(String col) throws DPFTRuntimeException {
		int cnt = count();
		for(int i = 0; i < cnt; i++){
			if(this.getDbo(i).getString("f_col").equalsIgnoreCase(col)){
				return this.getDbo(i).getString("datatype").equals("DATETIME");
			}
		}
		return false;
	}
	
	public boolean isNumber(String col) throws DPFTRuntimeException {
		int cnt = count();
		for(int i = 0; i < cnt; i++){
			if(this.getDbo(i).getString("f_col").equalsIgnoreCase(col)){
				return this.getDbo(i).getString("datatype").equals("NUMBER");
			}
		}
		return false;
	}

	public String normalizedDateString(String col, String value) throws DPFTRuntimeException {
		if(value == null || value.isEmpty())
			return "";
		int cnt = count();
		for(int i = 0; i < cnt; i++){
			if(this.getDbo(i).getString("f_col").equalsIgnoreCase(col)){
				SimpleDateFormat sdf = new SimpleDateFormat(this.getDbo(i).getString("datetime_format"));
				SimpleDateFormat sdf2 = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
				try {
					return sdf2.format(sdf.parse(value));
				} catch (ParseException e) {
					Object[] params = {value};
					throw new DPFTDboException("SYSTEM", "DPFT0024E", params, e);
				}
			}
		}
		return value;
	}
}

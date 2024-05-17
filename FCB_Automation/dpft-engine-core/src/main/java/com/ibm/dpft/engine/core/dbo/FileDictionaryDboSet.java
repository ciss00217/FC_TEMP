package com.ibm.dpft.engine.core.dbo;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class FileDictionaryDboSet extends DPFTDboSet {
	
	public FileDictionaryDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new FileDictionaryDbo(dboname, d, this);
	}

	public String[] getColumnsOrder() throws NumberFormatException, DPFTRuntimeException {
		int cnt = count();
		String[] rtnV = new String[cnt];
		for(int i = 0; i < cnt; i++){
			int index = Integer.valueOf(this.getDbo(i).getString("col_order"));
			index--;
			rtnV[index] = this.getDbo(i).getString("o_tbl_col");
		}
		return rtnV;
	}

	public HashMap<String, Integer> getColumnLengthSetting() throws NumberFormatException, DPFTRuntimeException {
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for(int i = 0; i < count(); i++){
			map.put(this.getDbo(i).getString("o_tbl_col"), Integer.valueOf(this.getDbo(i).getString("f_col_len")));
		}
		return map;
	}

	public String[] getNumericColumns() throws DPFTRuntimeException {
		int cnt = count();
		ArrayList<String> rtnV = new ArrayList<String>();
		int cc = 0;
		for(int i = 0; i < cnt; i++){
			if(this.getDbo(i).getString("is_numeric").equalsIgnoreCase("y")){
				rtnV.add(this.getDbo(i).getString("o_tbl_col"));
				cc++;
			}
		}
		return rtnV.toArray(new String[cc]);
	}

	public HashMap<String, String> getColumnNameDictionary() throws DPFTRuntimeException {
		HashMap<String, String> map = new HashMap<String, String>();
		for(int i = 0; i < count(); i++){
			map.put(this.getDbo(i).getString("o_tbl_col"), this.getDbo(i).getString("f_col"));
		}
		return map;
	}

	public HashMap<String, Boolean> setColFullWidthDefinition() throws DPFTRuntimeException {
		HashMap<String, Boolean> map = new HashMap<String, Boolean>();
		for(int i = 0; i < count(); i++){
			map.put(this.getDbo(i).getString("o_tbl_col"), Boolean.valueOf((this.getDbo(i).getString("full_width").equalsIgnoreCase("y"))));
		}
		return map;
	}

	public HashMap<String, String> getDateFormatDictionary() throws DPFTRuntimeException {
		HashMap<String, String> map = new HashMap<String, String>();
		for(int i = 0; i < count(); i++){
			if(this.getDbo(i).getString("is_datetime").equalsIgnoreCase("y"))
				map.put(this.getDbo(i).getString("o_tbl_col"), this.getDbo(i).getString("date_format"));
		}
		return map;
	}

}

package com.ibm.tfb.ext.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;

public class CCSManResDataFileReader extends DPFTFileReader {
	private final static Pattern fname_pattern = Pattern.compile("TMO([A-Z[a-z[0-9]]]{8})(\\d{2})[.]((20\\d{2}|19\\d{2})(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01]))");
	
	public CCSManResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
		super(dir, resFileDataLayoutDbo, chal_name);
		
	}

	@Override
	public String[] matchPattern(String file_pattern) throws DPFTRuntimeException {
		String[] list = super.matchPattern(file_pattern);
		ArrayList<String> rtnList = new ArrayList<String>();
		for(String fname: list){
			if(fname.indexOf("TMOINSC") == -1){
				rtnList.add(fname);
			}
		}
		return rtnList.toArray(new String[rtnList.size()]);
	}
	
	@Override
	public void write2TargetTable(String timestamp) throws DPFTRuntimeException {
		if(layout == null){
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0033E");
		}
		
		ResFileDataLayoutDetailDboSet layout_detail = layout.getLayoutDetail();
		if(layout_detail == null){
			Object[] params = {layout.getString("data_layout_id")};
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0031E", params);
		}
		DPFTDboSet targetSet = layout.getTargetTableDboSet();
		String dd = getDD();
		
		HashMap<String, String> f_col_2_tgt_col_map = layout.getFileColumns2TargetColumnsMapping();
		for(HashMap<String, String> rowdata: read_data){
			DPFTDbo new_data = targetSet.add();
			for(String col: rowdata.keySet()){
				if(f_col_2_tgt_col_map.get(col) == null)
					continue;
				new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
			}
			new_data.setValue("data_date", dd);
			new_data.setValue("all_usagecode", getAllUsageCode());
		}
		targetSet.save();
	}

	private String getAllUsageCode() {
		Matcher m = fname_pattern.matcher(fname);
		if(m.find()){
			return m.group(1);
		}
		return null;
	}

	private String getDD() {
		Matcher m = fname_pattern.matcher(fname);
		if(m.find()){
			return m.group(3);
		}
		return null;
	}
}

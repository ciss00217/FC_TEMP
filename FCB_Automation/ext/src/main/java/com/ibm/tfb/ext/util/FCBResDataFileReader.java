package com.ibm.tfb.ext.util;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class FCBResDataFileReader extends DPFTFileReader {

	public FCBResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
		super(dir, resFileDataLayoutDbo, chal_name);
		
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
		HashMap<String, String> f_col_2_tgt_col_map = layout.getFileColumns2TargetColumnsMapping();
		
		System.out.println("!!!!!!!!!!read_data:"+read_data.size());
		
		for(HashMap<String, String> rowdata: read_data){
			DPFTDbo new_data = targetSet.add();
			new_data.setValue("chal_name", chal_name);
			new_data.setValue("process_time", timestamp);
			for(String col: rowdata.keySet()){
				if(f_col_2_tgt_col_map.get(col) == null)
					continue;
				new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
			}
			
			DPFTDbo oFcb = getRelatedOutboundData(new_data);
			if(oFcb == null)
				continue;
			
			new_data.setValue("customer_id"   , oFcb.getString("customer_id"));
			new_data.setValue("res_date"      , getNormalizedDateString(new_data.getString("res_date")));
		}
	
		targetSet.save();
		targetSet.close();
	}

	private String getNormalizedDateString(String date_string) {
		if(date_string == null)
			return null;
		int digit = GlobalConstants.DFPT_DATETIME_FORMAT.length();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < digit - date_string.length(); i++){
			sb.append("0");
		}
		return date_string + sb.toString();
	}

	private DPFTDbo getRelatedOutboundData(DPFTDbo new_data) throws DPFTRuntimeException {
		DPFTDboSet set = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
							.getDboSet("O_" + chal_name, "treatment_code='" + new_data.getString("treatment_code") + "' and sys_no='" + new_data.getString("resv1") + "'");
		set.load();
		set.close();
		if(!set.isEmpty())
			return set.getDbo(0);
		return null;
	}
}

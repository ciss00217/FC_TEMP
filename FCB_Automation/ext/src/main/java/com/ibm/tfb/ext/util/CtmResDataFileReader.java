package com.ibm.tfb.ext.util;

import java.util.HashMap;

import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class CtmResDataFileReader extends DPFTFileReader {
	private static final String lz_type = "02";
	
	public CtmResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
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
		DPFTDboSet targetLzSet = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig())
									.getDboSet("RSP_MAIN_LZ", "rownum <= 10");
		HashMap<String, String> f_col_2_tgt_col_map = layout.getFileColumns2TargetColumnsMapping();
		for(HashMap<String, String> rowdata: read_data){
			if(rowdata.get("LEAD_SOURCE_ID") == null || rowdata.get("LEAD_SOURCE_ID").length() == 0)
				continue;
			
			DPFTDbo new_data = null;
			if(lz_type.indexOf(rowdata.get("LEAD_SOURCE_ID")) != -1)
				new_data = targetLzSet.add();
			else
				new_data = targetSet.add();
		
			new_data.setValue("chal_name", chal_name);
			new_data.setValue("process_time", timestamp);
			for(String col: rowdata.keySet()){
				if(f_col_2_tgt_col_map.get(col) == null)
					continue;
				if(col.equalsIgnoreCase("LEAD_SOURCE_ID")){
					if(lz_type.indexOf(rowdata.get("LEAD_SOURCE_ID")) != -1)
						new_data.setValue("lead_type", rowdata.get(col));
					else
						new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
				}else{
					new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
				}
			}
		}
		targetSet.save();
		targetLzSet.save();
		targetSet.close();
		targetLzSet.close();
	}
}

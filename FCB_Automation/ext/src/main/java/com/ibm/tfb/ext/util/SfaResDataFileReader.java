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

public class SfaResDataFileReader extends DPFTFileReader {
	private static final String lz_type = "05,06,07,08";

	public SfaResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
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
			DPFTDbo new_data = null;
			if(lz_type.indexOf(rowdata.get("LEAD_TYPE")) != -1)
				new_data = targetLzSet.add();
			else
				new_data = targetSet.add();
		
			new_data.setValue("chal_name", chal_name);
			new_data.setValue("process_time", timestamp);
			for(String col: rowdata.keySet()){
				if(f_col_2_tgt_col_map.get(col) == null)
					continue;
				if(col.equalsIgnoreCase("lead_type")){
					if(lz_type.indexOf(rowdata.get("LEAD_TYPE")) != -1)
						new_data.setValue("lead_type", rowdata.get(col));
					else
						new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
				}else{
					new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
				}
			}
			new_data.setValue("treatment_code", getTreatmentCode(rowdata.get("SFA_LEAD_ID")));
		}
		targetSet.save();
		targetLzSet.save();
		targetSet.close();
		targetLzSet.close();
	}

	private String getTreatmentCode(String sfa_lead_id) {
		String[] data = sfa_lead_id.split(GlobalConstants.FILE_DELIMETER_PIP);
		if(data.length == 2)
			return data[1];
		return null;
	}

}

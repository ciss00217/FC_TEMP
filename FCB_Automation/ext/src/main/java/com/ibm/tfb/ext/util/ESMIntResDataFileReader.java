package com.ibm.tfb.ext.util;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class ESMIntResDataFileReader extends DPFTFileReader {

	public ESMIntResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
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
		int i = 0;
		for(HashMap<String, String> rowdata: read_data){
			DPFTDbo new_data = targetSet.add();
			new_data.setValue("chal_name", chal_name);
			new_data.setValue("process_time", timestamp);
			new_data.setValue("res_code"    , "I");
			for(String col: rowdata.keySet()){
				if(f_col_2_tgt_col_map.get(col) == null)
					continue;
				new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
			}
			
			//Parse DestName field to get treatment code, customer_id
			String[] ds = new_data.getString("resv1").split(GlobalConstants.FILE_DELIMETER_PIP);
			if(ds.length == 3){
				DPFTConfig config = DPFTUtil.getSystemDBConfig();
				DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
				DPFTDboSet IDSet = (DPFTDboSet) connector.getDboSet("DPFT_IDMAPPING");
				IDSet.load();
				IDSet.filter("TREATMENT_CODE", ds[1]);
				// - recover customer_id (互動雙向簡訊)
				while( i < read_data.size()){
					DPFTDbo reload_id = IDSet.getDbo(i);
					if(reload_id.getColumnValue("id_number").equals(ds[2])){
						new_data.setValue("treatment_code", ds[1]);
						new_data.setValue("customer_id", reload_id.getColumnValue("customer_id"));
						new_data.setValue("resv1", ds[0]+"||"+ds[1]+"||"+reload_id.getColumnValue("customer_id"));
						break;
					}
				}
			}
			i++;
		}
		targetSet.save();
		targetSet.close();
	}

}

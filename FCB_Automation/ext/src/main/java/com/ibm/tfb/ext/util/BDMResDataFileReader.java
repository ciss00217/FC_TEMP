package com.ibm.tfb.ext.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTDataFormatException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.tfb.ext.common.TFBUtil;

public class BDMResDataFileReader extends DPFTFileReader {

	public BDMResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
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
		for(HashMap<String, String> rowdata: read_data){
			DPFTDbo new_data = targetSet.add();
			new_data.setValue("chal_name", chal_name);
			new_data.setValue("process_time", timestamp);
			new_data.setValue("res_date" , timestamp);
			new_data.setValue("res_code" , "UNDLV");
			for(String col: rowdata.keySet()){
				if(f_col_2_tgt_col_map.get(col) == null)
					continue;
				new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
			}
			
			DPFTDbo oBdm = getRelatedOutboundData(new_data, rowdata);
			if(oBdm == null)
				continue;
			
			new_data.setValue("customer_id"   , oBdm.getString("customer_id"));
			new_data.setValue("treatment_code", oBdm.getString("treatment_code"));
		}
		targetSet.save();
	}

	private DPFTDbo getRelatedOutboundData(DPFTDbo new_data, HashMap<String, String> rowdata) throws DPFTRuntimeException {
		DPFTDboSet oBdmSet = null;
		try {
			oBdmSet = new_data.getDboSet("O_BDM", buildWhereClauseString(rowdata));
		} catch (ParseException e) {
			throw new DPFTDataFormatException("SYSTEM", "DPFT0008E", e);
		}
		oBdmSet.load();
		oBdmSet.close();
		if(oBdmSet.isEmpty())
			return null;
		return oBdmSet.getDbo(0);
	}

	private String buildWhereClauseString(HashMap<String, String> rowdata) throws ParseException, DPFTRuntimeException {
		String where = "bill_type='%s' and bill_month='%s' and bill_seq='%s'";
		return String.format(where, rowdata.get("BILL_TYPE"), convert2ROCYearString(rowdata.get("BILL_MONTH")), rowdata.get("BILL_SEQ"));
	}

	private String convert2ROCYearString(String s) throws ParseException, DPFTRuntimeException {
		if(s.length() != GlobalConstants.DPFT_MONTH_FORMAT.length()){
			throw new DPFTDataFormatException("CUSTOM", "TFB00005E");
		}
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DPFT_MONTH_FORMAT);
		return TFBUtil.getROCYearMonthString(sdf.parse(s));
	}

}

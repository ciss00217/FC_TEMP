package com.ibm.tfb.ext.util;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class BSMResDataFileReader extends DPFTFileReader {

	public BSMResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
		super(dir, resFileDataLayoutDbo, chal_name);
	}

	@Override
	public void write2TargetTable(String timestamp) throws DPFTRuntimeException {
		if (layout == null) {
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0033E");
		}

		ResFileDataLayoutDetailDboSet layout_detail = layout.getLayoutDetail();
		if (layout_detail == null) {
			Object[] params = { layout.getString("data_layout_id") };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0031E", params);
		}

		DPFTDboSet targetSet = layout.getTargetTableDboSet();
		HashMap<String, String> f_col_2_tgt_col_map = layout.getFileColumns2TargetColumnsMapping();
		String serviceType = null;
		if (!this.headerString.isEmpty()) {
			serviceType = this.headerString.substring(this.headerString.length() - 2, this.headerString.length());
		}
		for (HashMap<String, String> rowdata : read_data) {
			DPFTDbo new_data = targetSet.add();
			if (serviceType != null) {
				new_data.setValue("RESV3", serviceType);
			}
			new_data.setValue("chal_name", chal_name);
			new_data.setValue("process_time", timestamp);
			new_data.setValue("RES_DATE", this.fname.substring(0, 8) + "000000");

			for (String col : rowdata.keySet()) {
				if (f_col_2_tgt_col_map.get(col) == null) {
					continue;
				}
				if (col.equals("CUSTOMER_ID")) {
					String[] CUSTOMER_ID = rowdata.get(col).split("\\|");
					if (CUSTOMER_ID != null && CUSTOMER_ID.length > 1) {
						new_data.setValue("TREATMENT_CODE", CUSTOMER_ID[1]);
						new_data.setValue("CUSTOMER_ID", CUSTOMER_ID[0]);
					}
				} else {
					new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
				}
			}
		}
		targetSet.save();
		targetSet.close();
	}
}
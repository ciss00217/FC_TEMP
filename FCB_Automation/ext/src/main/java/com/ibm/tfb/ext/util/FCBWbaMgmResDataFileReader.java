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
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBConstants;
import com.ibm.tfb.ext.common.TFBUtil;

public class FCBWbaMgmResDataFileReader extends DPFTFileReader {

	public FCBWbaMgmResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
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
		
		DPFTConfig config = DPFTUtil.getSystemDBConfig();
		DPFTConnector connectorDID = DPFTConnectionFactory.initDPFTConnector(config);
		
		// Generate random seq for cust_id
		String[] seq_cust_list = TFBUtil.generateSEQ(TFBConstants.WBA_SEQ_CUST_ID, read_data.size());
		int j = 0;
		DPFTLogger.info(this, "timestamp=" + timestamp + ",fname=" + fname);
		for (HashMap<String, String> rowdata : read_data) {
			DPFTDbo new_data = targetSet.add();
			new_data.setValue("process_time", timestamp);
			new_data.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_STAGE);
			
			DPFTLogger.debug(this, "HAVE_DID column is: "+rowdata.get("HAVE_DID")+" and DID column is: "+rowdata.get("DID"));
			// Map Referral Customer ID if exists
			if ( "Y".equals(rowdata.get("HAVE_DID")) && !"undefined".equals(rowdata.get("DID"))) {
				StringBuilder sbpromotion = new StringBuilder();
				sbpromotion.append("PROMOTION_CODE = \'");
				sbpromotion.append(rowdata.get("DID"));
				sbpromotion.append("\'");
				DPFTDboSet promotionCodeToIDSet = (DPFTDboSet) connectorDID.getDboSet(" CUST_ID ", "CMETL.CFMBSEL_STG", sbpromotion.toString());
				DPFTLogger.debug(this, "promotionCodeSet DBOSIZEHERE " + String.valueOf(promotionCodeToIDSet.count()));
				if ( promotionCodeToIDSet.count() > 0 ) {
					new_data.setValue("DID_ID", promotionCodeToIDSet.getDbo(0).getString("CUST_ID").trim());
				}
			}
			
			new_data.setValue("CUSTOMER_ID", seq_cust_list[j]);
			j+=1;		
			//DPFTLogger.debug(this,"!!!!!!!!!!!!!!!!!!f_col_2_tgt_col_map:" + f_col_2_tgt_col_map);
			//DPFTLogger.debug(this,"!!!!!!!!!!!!!!!!!!rowdata:" + rowdata);

			for (String col : rowdata.keySet()) {
				if (f_col_2_tgt_col_map.get(col) == null)
					continue;
				new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
			}

		}
		targetSet.save();
		targetSet.close();
		targetSet.clear();
	}
}

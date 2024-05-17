package com.ibm.tfb.ext.util;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;

public class SMSFileFormatter extends DPFTCSVFileFormatter {
	private String header = null;

	public SMSFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta, String header) {
		super(h_meta, d_meta);
		this.header = header;
	}
	
	@Override
	public void format(DPFTDboSet rs) throws DPFTRuntimeException {
		super.format(rs);
		String data_string = this.getDataFileString();
		if(data_string != null){
			//concat SMS header at first line
			StringBuilder sb = new StringBuilder();
			sb.append(header).append(GlobalConstants.FILE_EOL).append(data_string);
			this.setDataFileString(sb.toString());
		}
	}

}

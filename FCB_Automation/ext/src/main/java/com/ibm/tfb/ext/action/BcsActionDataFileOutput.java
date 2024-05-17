package com.ibm.tfb.ext.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.tfb.ext.util.BcsFileFormatter;

public class BcsActionDataFileOutput extends DPFTActionDataFileOutput {
	@Override
	public DPFTFileFormatter getFileFormatter() throws DPFTRuntimeException {
		return new BcsFileFormatter(new DPFTFileMetaData(meta), new DPFTFileMetaData(meta, dicSet));
	}
	
	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

	@Override
	public String getChannelName() {
		return "BCS";
	}

}
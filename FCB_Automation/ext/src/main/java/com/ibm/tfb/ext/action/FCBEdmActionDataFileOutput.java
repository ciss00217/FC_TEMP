package com.ibm.tfb.ext.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.util.BemFileFormatter;
import com.ibm.tfb.ext.util.FCBEdmFileFormatter;
import com.ibm.tfb.ext.util.FCBLinFileFormatter;

public class FCBEdmActionDataFileOutput extends DPFTActionDataFileOutput {

	@Override
	public DPFTFileFormatter getFileFormatter() throws DPFTRuntimeException {
		return new FCBEdmFileFormatter(new DPFTFileMetaData(meta), new DPFTFileMetaData(meta, dicSet));
	}
	
	@Override
	public String getChannelName() {
		return "EDM";
	}

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

}

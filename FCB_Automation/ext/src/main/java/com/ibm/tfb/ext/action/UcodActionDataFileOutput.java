package com.ibm.tfb.ext.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicFileOutput;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;

public class UcodActionDataFileOutput extends DPFTActionObndPeriodicFileOutput {
	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		
		return null;
	}

	@Override
	public String getChannelName() {
		
		return "UCOD";
	}

	@Override
	public boolean needNotification() {
		return false;
	}


}

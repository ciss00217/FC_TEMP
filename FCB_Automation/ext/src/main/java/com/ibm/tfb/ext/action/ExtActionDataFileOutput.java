package com.ibm.tfb.ext.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;

import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;

public class ExtActionDataFileOutput extends DPFTActionDataFileOutput {
	private String chalCode = "";

	public ExtActionDataFileOutput(String chalCode) {
		super();
		this.chalCode = chalCode;
	}

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

	@Override
	public String getChannelName() {
		return chalCode;
	}
}

package com.ibm.tfb.ext.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;

public class FCBEomFileFormatter extends DPFTCSVFileFormatter {

	public FCBEomFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
	}
	
	@Override
	protected String getDateString(Date date, String dateFormat) {
		dateFormat = "yyyyMMdd";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}
}

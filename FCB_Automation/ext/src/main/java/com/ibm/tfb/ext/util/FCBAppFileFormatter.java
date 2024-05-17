package com.ibm.tfb.ext.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class FCBAppFileFormatter extends DPFTCSVFileFormatter {

	public FCBAppFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
	}
	
	@Override
	protected String getStringWithoutDelimeter(Object columnValue) {
		if (columnValue == null)
			return "";
		if (columnValue instanceof String) {
			String s = (String) columnValue;
			//if (getDelimeter().equals(GlobalConstants.FILE_DELIMETER_COMMA))
			//	return s.replaceAll(GlobalConstants.FILE_DELIMETER_COMMA, GlobalConstants.FILE_DELIMETER_COMMA_FULL);
			//else if (getDelimeter().equals(GlobalConstants.FILE_DELIMETER_SEMICOLON))
			//	return s.replaceAll(GlobalConstants.FILE_DELIMETER_SEMICOLON,
			//			GlobalConstants.FILE_DELIMETER_SEMICOLON_FULL);
			return s;
		}
		return null;
	}
	
	@Override
	protected String buildHeaderString(String[] cols) {

		StringBuilder sb = new StringBuilder();
		sb.append("#");
		for (String col : cols) {
			sb.append(col).append(getDelimeter());
		}
		DPFTLogger.debug(this, "buildHeaderString : " + sb.toString());
		return sb.substring(0, sb.length() - 1);
	}
	
	@Override
	protected String getDateString(Date date, String dateFormat) {
		dateFormat = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}
}

package com.ibm.tfb.ext.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;

public class FCBLinFileFormatter extends DPFTCSVFileFormatter {

	public FCBLinFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
	}
	
	@Override
	protected void formatControlFileString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		DPFTFileMetaData meta = this.getControlFileMeta();
		
		// Add D file name to control file string
		sb.append(this.getDataFileName()).append(getDelimeter());
		
		if (getDataFileMeta().isStaticColLength()) {
			if (meta.getActionID() != null)
				sb.append(meta.getActionID()).append(String.format("%1$010d", num_output));
			else
				sb.append(String.format("%1$010d", num_output));
		} else {
			/* build data header String */
			if (meta.isContainHeader()) {
				if (meta.getActionID() != null)
					sb.append(buildHeaderString(GlobalConstants.H_HEADER)).append(GlobalConstants.FILE_EOL);
				else
					sb.append(buildHeaderString(GlobalConstants.H_HEADER2)).append(GlobalConstants.FILE_EOL);
			}
			if (meta.getActionID() != null)
				sb.append(meta.getActionID()).append(getDelimeter()).append(num_output);
			else
				sb.append(num_output);
		}
		this.setControlFileString(sb.toString());

		/* build control file name */
		this.setControlFileName(buildFileNameByPattern(meta.getFileName(), rs.getDbo(0)));
	}
	
	@Override
	protected String getDateString(Date date, String dateFormat) {
		dateFormat = "yyyyMMdd HH:mm";
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}
}

package com.ibm.tfb.ext.util;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;

public class FCBDcsFileFormatter extends DPFTCSVFileFormatter {

	public FCBDcsFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
	}

	@Override
	protected void formatControlFileString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		DPFTFileMetaData meta = this.getControlFileMeta();

		// Add D file name to control file string
		sb.append(this.getDataFileName().replace(".D", "")).append(getDelimeter());

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
	public HashMap<String, String> getFormatFileList() {
		HashMap<String, String> file_out_list = new HashMap<String, String>();
		// if (!StringUtils.isEmpty(getDataFileString())) {
		if (hasControlFile()) {
			if (!StringUtils.isEmpty(getDataFileString())) {
				file_out_list.put(getDataFileName(), getDataFileString());
				file_out_list.put(getControlFileName(), getControlFileString());
			} else {
				file_out_list.put(getDataFileName(), "");
				file_out_list.put(getControlFileName(), getControlFileString());
			}
		} else {
			file_out_list.put(getDataFileName(), getDataFileString());
		}

		/* has z file for encryption */
		if (hasZFile()) {
			file_out_list.put(getZFileName(), getZFileString());
		}
		// }
		return file_out_list;
	}
}

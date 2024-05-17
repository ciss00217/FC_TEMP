package com.ibm.tfb.ext.util;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class BcsFileFormatter extends DPFTCSVFileFormatter {

	public BcsFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
	}

	protected void formatDataString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		DPFTFileMetaData meta = this.getDataFileMeta();
		/* build data header String */
		if (meta.isContainHeader()) {
			sb.append(buildHeaderString(meta.getFileColsInOrder())).append(GlobalConstants.FILE_EOL);
		}

		/* build data body */
		num_output = 0;
		for (int i = 0; i < rs.count(); i++) {
			if (!canOutputRecord(rs.getDbo(i)))
				continue;
			if (i == rs.count() - 1) {
				sb.append(buildDataString(rs.getDbo(i), meta, true, 1));
			} else {
				sb.append(buildDataString(rs.getDbo(i), meta, true, 1)).append(GlobalConstants.FILE_EOL);
			}
			num_output++;
		}

		/* if file transfer validation active, append validation String */
		if (meta.needTransferValidation()) {
			String FILE_TRF_CNST = GlobalConstants.FILE_TRF_CNST;
			if (!StringUtils.isEmpty(meta.getTrf_cnst())) {
				FILE_TRF_CNST = meta.getTrf_cnst();
			}
			if (meta.getStaticTrf_cnst() > 0) {
				sb.append(FILE_TRF_CNST).append(String.format("%1$0" + meta.getStaticTrf_cnst() + "d", num_output));
			} else {
				sb.append(FILE_TRF_CNST).append(num_output);
			}
		}
		if (num_output != 0) {
			this.setDataFileString(sb.toString());
		}
		/* build data file name */
		this.setDataFileName(buildFileNameByPattern(meta.getFileName(), rs.getDbo(0)));

		/* build compression file name if needed */
		if (meta.hasCompressFilePattern()) {
			this.setCompressFileName(buildFileNameByPattern(meta.getCompressFilePattern(), rs.getDbo(0)));

		}
	}
}

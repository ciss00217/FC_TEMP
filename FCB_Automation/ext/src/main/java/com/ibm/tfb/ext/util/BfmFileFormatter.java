package com.ibm.tfb.ext.util;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;

public class BfmFileFormatter extends DPFTCSVFileFormatter {

	public BfmFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
	}

	@Override
	protected String buildDataString(DPFTDbo dbo, DPFTFileMetaData meta) throws DPFTRuntimeException {
		return super.buildDataString(dbo, meta, true, 1);
	}

}

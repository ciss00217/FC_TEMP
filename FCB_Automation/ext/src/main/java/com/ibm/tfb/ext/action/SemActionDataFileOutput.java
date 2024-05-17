package com.ibm.tfb.ext.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.util.BemFileFormatter;

public class SemActionDataFileOutput extends DPFTActionDataFileOutput {
	@Override
	public DPFTFileFormatter getFileFormatter() throws DPFTRuntimeException {
		// Mail_subject | 版型代碼 | Mail_Close_Email | Start_DateTime(形式為yyyy/MM/dd HH:mm:ss)
		String[] header = { "MAIL_SUBJECT", "MAIL_TEMPLATE_ID", "CONTACTEMAIL", "SEND_DATE" };
		return new BemFileFormatter(new DPFTFileMetaData(meta), new DPFTFileMetaData(meta, dicSet),
				TFBUtil.buildMultiLineHeader(header, this.getPreviousAction().getResultSet().getDbo(0),
						GlobalConstants.FILE_DELIMETER_SPIP));
	}

	@Override
	public String getChannelName() {

		return "SEM";
	}

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

}

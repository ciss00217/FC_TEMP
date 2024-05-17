package com.ibm.tfb.ext.dbo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class TFBUsageCodeDboSet extends DPFTDboSet {
	public TFBUsageCodeDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	private String process_time = null;

	
	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new TFBUsageCodeDbo(dboname, d, this);
	}
	
	@Override
	public DPFTDbo add() throws DPFTRuntimeException {
		DPFTDbo new_dbo = super.add();
		if(process_time == null)
			process_time = getProcessTimestamp();
		new_dbo.setValue("process_time", process_time);
		return new_dbo;
	}
	
	private String getProcessTimestamp() {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		return sdf.format(date);
	}

}

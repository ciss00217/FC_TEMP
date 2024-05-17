package com.ibm.dpft.engine.core.util;

import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public abstract class DPFTDataReader {
	protected ResFileDataLayoutDbo layout = null;
	
	abstract boolean read(String datasrc) throws DPFTRuntimeException;
	abstract void write2TargetTable(String process_time) throws DPFTRuntimeException;
	
	
	public ResFileDataLayoutDbo getDataLayout() {
		return layout;
	}

	public void setDataLayout(ResFileDataLayoutDbo layout) {
		this.layout = layout;
	}
	
}

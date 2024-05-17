package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class ResFileDataLayoutDbo extends DPFTDbo {
	private ResFileDataLayoutDetailDboSet detail = null;

	public ResFileDataLayoutDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);

	}

	public ResFileDataLayoutDetailDboSet getLayoutDetail() throws DPFTRuntimeException {
		if (detail == null) {
			ResFileDataLayoutDetailDboSet set = (ResFileDataLayoutDetailDboSet) this.getDboSet(
					"DPFT_RES_DATA_LAYOUT_DETAIL", "data_layout_id='" + this.getString("data_layout_id") + "'");
			set.load();
			set.close();
			detail = set;
		}
		return detail;
	}

	public String getDelimeter() {
		String delimeter = this.getString("delimeter");
		if (delimeter == null)
			return GlobalConstants.FILE_DELIMETER_COMMA;
		if (delimeter.equals("AND"))
			return GlobalConstants.FILE_DELIMETER_AND;
		else if (delimeter.equals("SEMICOLON"))
			return GlobalConstants.FILE_DELIMETER_SEMICOLON;
		else if (delimeter.equals("TAB"))
			return GlobalConstants.FILE_DELIMETER_TAB;
		else if (delimeter.equals("SPIP"))
			return GlobalConstants.FILE_DELIMETER_SPIP;
		else if (delimeter.equals("ESPIP"))
			return GlobalConstants.FILE_DELIMETER_ESPIP;
		else if (delimeter.equals("EMPTY"))
			return GlobalConstants.FILE_DELIMETER_EMPTY;

		return GlobalConstants.FILE_DELIMETER_COMMA;
	}

	public DPFTDboSet getTargetTableDboSet() throws DPFTRuntimeException {
		return this.getDboSet(this.getString("target_tbl"), "rownum <= 10");
		// return this.getDboSet("TOP 10 *", this.getString("target_tbl"), "");
	}

	public HashMap<String, String> getFileColumns2TargetColumnsMapping() throws DPFTRuntimeException {
		if (detail != null) {
			return detail.getColumnsMapping();
		}
		return null;
	}

	public boolean isStaticMode() {
		return this.getString("static_mode").equalsIgnoreCase("y");
	}

	public String getEncoding() {
		return this.getString("encode");
	}

}

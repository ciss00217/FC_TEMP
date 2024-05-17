package com.ibm.tfb.ext.util;

public class FCBSmsReceive {
	public String RowId;
	public String ErrorCode;
	public String CheckMode;

	public String getCheckMode() {
		return CheckMode;
	}

	public void setCheckMode(String checkMode) {
		CheckMode = checkMode;
	}

	public String getRowId() {
		return RowId;
	}

	public void setRowId(String rowId) {
		RowId = rowId;
	}

	public String getErrorCode() {
		return ErrorCode;
	}

	public void setErrorCode(String errorCode) {
		ErrorCode = errorCode;
	}
}

package com.ibm.dpft.engine.core.action;

public class DPFTActionInstaLocalFileWatch extends DPFTActionLocalFileWatch {
	@Override
	public String getTableWatchCriteria() {
		return "active=1 and insta=1";
	}
}

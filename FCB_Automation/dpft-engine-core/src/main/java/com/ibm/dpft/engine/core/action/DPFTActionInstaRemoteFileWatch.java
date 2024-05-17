package com.ibm.dpft.engine.core.action;

public class DPFTActionInstaRemoteFileWatch extends DPFTActionRemoteFileWatch {
	@Override
	public String getTableWatchCriteria() {
		return "active=1 and insta=1";
	}
}

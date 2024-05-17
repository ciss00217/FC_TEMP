package com.ibm.dpft.engine.core.util;

import java.util.Comparator;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;

public abstract class DPFTDboComparator implements Comparator<DPFTDbo> {
	private String sort_column = null;
	private int order = 0;

	public DPFTDboComparator(String col, int order) {
		setSortColumn(col);
		setOrder(order);
	}

	public String getSortColumn() {
		return sort_column;
	}

	public void setSortColumn(String sort_column) {
		this.sort_column = sort_column;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}
}

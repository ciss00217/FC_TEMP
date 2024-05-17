package com.ibm.dpft.engine.core.util;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;

public class DPFTNumberComparator extends DPFTDboComparator{

	public DPFTNumberComparator(String col, int order) {
		super(col, order);
	}

	@Override
	public int compare(DPFTDbo o1, DPFTDbo o2) {
		int d1 = Integer.valueOf(o1.getString(getSortColumn()));
		int d2 = Integer.valueOf(o2.getString(getSortColumn()));
		if(getOrder() == DPFTDboSet.ORDER_ASC)
			return d1 - d2;
		else
			return d2 - d1;
	}

}

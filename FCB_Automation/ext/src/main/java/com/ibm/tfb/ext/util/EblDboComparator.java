package com.ibm.tfb.ext.util;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.util.DPFTDboComparator;

public class EblDboComparator extends DPFTDboComparator {

	public EblDboComparator() {
		super(null, DPFTDboSet.ORDER_ASC);
		
	}

	@Override
	public int compare(DPFTDbo o1, DPFTDbo o2) {
		int cr = o1.getString("customer_id").compareTo(o2.getString("customer_id"));
		if(cr != 0)
			return cr;
		cr = Integer.valueOf(o1.getString("priority_id")) - Integer.valueOf(o2.getString("priority_id"));
		return cr;
	}

}

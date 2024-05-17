package com.ibm.dpft.engine.core.util;

import java.util.Date;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;

public class DPFTDatetimeComparator extends DPFTDboComparator {
	
	public DPFTDatetimeComparator(String col, int order) {
		super(col, order);
	}

	@Override
	public int compare(DPFTDbo o1, DPFTDbo o2) {
		Date d1 = null;
		Date d2 = null;
		try{
			d1 = o1.getDate(getSortColumn());
			d2 = o2.getDate(getSortColumn());
		}catch(Exception e){
			DPFTLogger.error(this, "Error when compare", e);
		}
		if(getOrder() == DPFTDboSet.ORDER_ASC)
			return d1.compareTo(d2);
		else
			return d2.compareTo(d1);
	}
}

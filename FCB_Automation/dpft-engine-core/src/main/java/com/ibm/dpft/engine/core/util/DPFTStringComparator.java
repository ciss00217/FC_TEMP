package com.ibm.dpft.engine.core.util;

import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;

public class DPFTStringComparator extends DPFTDboComparator {

	public DPFTStringComparator(String col, int order) {
		super(col, order);
		
	}

	@Override
	public int compare(DPFTDbo o1, DPFTDbo o2) {
		String s1 = o1.getString(getSortColumn());
		String s2 = o2.getString(getSortColumn());
		if(getOrder() == DPFTDboSet.ORDER_ASC){
			if(s1.length() == s2.length())
				return s1.compareTo(s2);
			else if(s1.length() < s2.length())
				return -1;
			else
				return 1;
		}else{
			if(s1.length() == s2.length())
				return s2.compareTo(s1);
			else if(s1.length() < s2.length())
				return 1;
			else
				return -1;
		}
	}

}

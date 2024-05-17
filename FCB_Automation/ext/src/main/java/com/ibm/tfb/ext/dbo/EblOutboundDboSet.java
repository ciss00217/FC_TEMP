package com.ibm.tfb.ext.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.tfb.ext.common.TFBConstants;
import com.ibm.tfb.ext.util.EblDboComparator;

public class EblOutboundDboSet extends DPFTOutboundDboSet {
	public EblOutboundDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	private HashMap<String, Integer> custMap = new HashMap<String, Integer>();
	private final static int MAX_OUTPUT_THRESHOLD_PER_CUST = 10;

	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new EblOutboundDbo(dboname, d, this);
	}

	public void processOutputRecords() throws DPFTRuntimeException {
		this.orderby("priority_id", DPFTDboSet.TYPE_NUMBER, DPFTDboSet.ORDER_ASC);
		for(int i = 0; i < count(); i++){
			String customer_id = this.getDbo(i).getString("customer_id");
			if(!custMap.containsKey(customer_id)){
				custMap.put(customer_id, 1);
				continue;
			}
			
			int cust_count = custMap.get(customer_id);
			if(cust_count == MAX_OUTPUT_THRESHOLD_PER_CUST){
				this.getDbo(i).setValue("process_status", TFBConstants.EBL_PROCESS_STAT_IGNORE);
			}else{
				cust_count++;
				custMap.put(customer_id, cust_count);
			}
		}
		this.save(false);
		
		//order by cust_id, priority_id
		this.orderby(new EblDboComparator());
	}
}

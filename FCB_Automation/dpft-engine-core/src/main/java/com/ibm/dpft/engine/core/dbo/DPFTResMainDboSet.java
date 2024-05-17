package com.ibm.dpft.engine.core.dbo;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTResMainDboSet extends DPFTDboSet {

	public DPFTResMainDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	private boolean needAdd2Del = true;
	private int     valid_count = 0;
	
	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTResMainDbo(dboname, d, this);
	}
	
	@Override
	public DPFTDbo add() throws DPFTRuntimeException {
		DPFTDbo new_dbo = super.add();
		
		//init value
		new_dbo.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_STAGE);
		return new_dbo;
	}
	
	@Override
	public void save() throws DPFTRuntimeException {
		//remove invalid records, and move to rsp_main_del
		DPFTDboSet delSet = this.getDBConnector().getDboSet("RSP_MAIN_DEL", "rownum <= 10");
//		DPFTDboSet delSet = this.getDBConnector().getDboSet("TOP 10 *", "RSP_MAIN_DEL", "");
		valid_count = 0;
		for(int i = 0; i < count(); i++){
			if(this.getDbo(i).tobeAdded()){
				validate(this.getDbo(i));
				if(this.getDbo(i).tobeDeleted() && needAdd2Del ){
					//add to rsp_main_del
					DPFTDbo d = delSet.add();
					d.setValue(this.getDbo(i));
				}
			}
		}
		delSet.save();
		delSet.close();
		super.save();
	}
	
	public void setNeedAdd2DelTableFlg(boolean flg){
		this.needAdd2Del = flg;
	}

	private void validate(DPFTDbo dbo) {
		if(dbo.isNull("customer_id") || dbo.isNull("treatment_code")){
			dbo.delete();
			return;
		}
		valid_count++;
	}

	public int getValidResCount() {
		return valid_count;
	}

	public void setValidResCount(int count) {
		this.valid_count = count;
	}

}

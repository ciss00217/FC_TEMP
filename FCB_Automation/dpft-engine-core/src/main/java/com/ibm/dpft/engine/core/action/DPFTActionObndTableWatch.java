package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionObndTableWatch extends DPFTActionTableWatch {
	private final static long sleep_time = 60000;
	
	public DPFTActionObndTableWatch(){
		super();
		
		/*set watch intervals to 1 min*/
		this.setPostActionSleepTime(sleep_time);
	}
	
	@Override
	public void finish() throws DPFTRuntimeException {
		/*lock record by setting process status = 'R'*/
		DPFTOutboundControlDboSet set = (DPFTOutboundControlDboSet) this.getDataSet();
		set.run();
		super.finish();
	}

	@Override
	public DPFTConfig getDBConfig() {
		
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		
		return "H_OUTBOUND";
	}

	@Override
	public String getTableWatchCriteria() {
		//return "process_status='" + GlobalConstants.DPFT_OBND_STAT_STAGE + "' and (ftp_time is null or ftp_time='') and insta=0 ";

	return "process_status='" + GlobalConstants.DPFT_OBND_STAT_STAGE + "' and (ftp_time is null or ftp_time='') and insta=0  and rownum <= (5 - (select count(*) from h_outbound where process_status='R'))";

	}

	@Override
	public String getTriggerKeyCol() {
		
		return "chal_name";
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		if(getRunnableCtrlSet() == null){
			//Wait and Sleep
			this.setPostActionSleepTime(sleep_time);
			this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_RUN);
			this.getDataSet().close();
			if(!this.getDataSet().isEmpty())
				DPFTLogger.info(this, "Still waiting for another running thread to finish processing on Inbound Data...");
		}
	}

	private DPFTDboSet getRunnableCtrlSet() throws DPFTRuntimeException {
		DPFTOutboundControlDboSet dboset = (DPFTOutboundControlDboSet) this.getDataSet();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < dboset.count(); i++){
			DPFTOutboundControlDbo dbo = (DPFTOutboundControlDbo) dboset.getDbo(i);
			if(dbo.isRunnable() && sb.indexOf(dbo.getString("chal_name")) == -1){
				sb.append("'").append(dbo.getString("chal_name")).append("',");
			}
		}
		if(sb.length() > 0){
			dboset.reset(getTableWatchCriteria() + " and chal_name in (" + sb.substring(0, sb.length()-1) + ")");
			dboset.load();
			return dboset;
		}
		return null;
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		DPFTOutboundControlDboSet set = (DPFTOutboundControlDboSet) this.getDataSet();		
		set.error();
	}
}

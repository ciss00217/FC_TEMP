package com.ibm.dpft.engine.core.action;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDbo;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTFileReadException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionResControlTableWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		
		return "H_INBOUND_RES";
	}

	@Override
	public String getTableWatchCriteria() {
		
		return "process_status='" + GlobalConstants.DPFT_CTRL_STAT_RUN + "' and insta=0";
	}

	@Override
	public String getTriggerKeyCol() {
		
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		
		//h file
		DPFTInboundControlDboSet hSet = (DPFTInboundControlDboSet) this.getDataSet();
		if(hSet.isEmpty()){
			System.out.println("hSet.isEmpty()");
			return;
		}
		
		// d file
		ResFileDirSettingDboSet fSet = (ResFileDirSettingDboSet) this.getPreviousAction().getResultSet();
		HashMap<String, DPFTFileReader> drs = new HashMap<String, DPFTFileReader>();
		for(int i = 0; i < fSet.count(); i++){
			ResFileDirSettingDbo f = (ResFileDirSettingDbo) fSet.getDbo(i);
			DPFTFileReader reader = f.getLocalFileReader();
			String d_file_name = f.getDataFileName();
			
			if(reader.isPattern(d_file_name)){
				String[] flist = reader.matchPattern(d_file_name);
				for(String fname: flist){
					drs.put(fname, reader);
				}
			}else{
				drs.put(d_file_name, reader);
			}
		}
		
		String timestamp = DPFTUtil.getCurrentTimeStampAsString();
		for(int i = 0; i < hSet.count(); i++){
			DPFTInboundControlDbo h = (DPFTInboundControlDbo) hSet.getDbo(i);
			String d_file_name = h.getString("d_file");
			DPFTFileReader data_reader = drs.get(d_file_name);
			if(data_reader == null)
				continue;
			try{
				if(data_reader.isPattern(d_file_name)){
					String[] flist = data_reader.matchPattern(d_file_name);
					for(String fname: flist){
						if(data_reader.read(fname)){
							data_reader.write2TargetTable(timestamp);
							h.setValue("process_time", timestamp);
							h.complete();
						}
					}
				}else{
					if(!data_reader.exist(d_file_name)){
						Object[] params = {d_file_name};
						throw new DPFTFileReadException("CUSTOM", "TFB00014E", params);
					}
					
					if(data_reader.read(d_file_name)){
						data_reader.write2TargetTable(timestamp);
						h.setValue("process_time", timestamp);
						h.complete();
					}
				}
			}catch(Exception e){
				h.error();
				//thread interrupted throw out InterruptedException
				if(e instanceof InterruptedException)
					throw e;
				
				if(e instanceof DPFTRuntimeException){
					((DPFTRuntimeException) e).handleException();
				}else{
					Object[] params = {d_file_name};
					DPFTFileReadException ex = new DPFTFileReadException("SYSTEM", "DPFT0032E", params, e);
					ex.handleException();
				}
			}
			
		}
		hSet.save();
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}
}

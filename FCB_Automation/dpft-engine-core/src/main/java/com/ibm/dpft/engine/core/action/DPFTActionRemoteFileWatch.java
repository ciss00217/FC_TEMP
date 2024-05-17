package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.dpft.engine.core.util.DPFTcdFTPUtil;

public class DPFTActionRemoteFileWatch extends DPFTActionTableWatch {

	@Override
	public DPFTConfig getDBConfig() {
		return DPFTUtil.getSystemDBConfig();
	}

	@Override
	public String getTableName() {
		return "DPFT_RES_FTP_DIR_DEF";
	}

	@Override
	public String getTableWatchCriteria() {
		return "active=1 and insta=0";
	}

	@Override
	public String getTriggerKeyCol() {
		return null;
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		ResFileDirSettingDboSet fSet = (ResFileDirSettingDboSet) this.getDataSet();
		for(int i = 0; i < fSet.count(); i++){
			DPFTcdFTPUtil cdutil = null;
			try{
				ResFileDirSettingDbo dbo = (ResFileDirSettingDbo) fSet.getDbo(i);
				cdutil = new DPFTcdFTPUtil(dbo.getString("ldir")
										 , dbo.getString("dir")
										 , dbo.getString("cd_profile"));
				String[] flist = dbo.getRemoteFileName();
				String[] clist = dbo.getRemoteCtrlName();
//				cdutil.lock();
				int rtnCode = cdutil.doFTP_Get(clist, flist);
				if(rtnCode == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS)
					deleteRemoteFiles(cdutil, dbo, clist, flist);
//				cdutil.unlock();
			}catch(Exception e){
//				if(cdutil != null)cdutil.unlock();
				
				//thread interrupted throw out InterruptedException
				if(e instanceof InterruptedException)
					throw e;
				
				if(e instanceof DPFTRuntimeException){
					((DPFTRuntimeException)e).handleException();
				}else{
					DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
					ex.handleException();
				}
			}
		}
	}

	private void deleteRemoteFiles(DPFTcdFTPUtil cdutil, ResFileDirSettingDbo dbo, String[] clist, String[] flist) throws DPFTRuntimeException {
		String[] del_clist = null;
		String[] del_flist = null;
		if(clist[0] != null){
			DPFTFileReader creader = dbo.getLocalControlFileReader();
			if(creader.isPattern(clist[0])){
				del_clist = creader.matchPattern(clist[0]);
			}else{
				del_clist = clist;
			}
		}
		DPFTFileReader dreader = dbo.getLocalFileReader();
		if(dreader.isPattern(flist[0])){
			del_flist = dreader.matchPattern(flist[0]);
		}else{
			del_flist = flist;
		}
		cdutil.doFTP_Del(del_clist, del_flist);		
	}
	
	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		
		
	}

	@Override
	public String getSelectAttrs() {
		// TODO Auto-generated method stub
		return null;
	}

}

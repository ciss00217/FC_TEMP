package com.ibm.dpft.engine.core.action;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDbo;
import com.ibm.dpft.engine.core.dbo.DPFTInboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDboSet;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTActionLocalFileWatch extends DPFTActionTableWatch {

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
		DPFTInboundControlDboSet hSet = (DPFTInboundControlDboSet) getRespondControlSet();

		for (int i = 0; i < fSet.count(); i++) {
			try {
				// d&h兩者需到齊才跑
				readLocalFiles((ResFileDirSettingDbo) fSet.getDbo(i), hSet);
			} catch (Exception e) {
				// thread interrupted throw out InterruptedException
				if (e instanceof InterruptedException)
					throw e;

				if (e instanceof DPFTRuntimeException) {
					((DPFTRuntimeException) e).handleException();
				} else {
					DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
					ex.handleException();
				}
			}

		}
		hSet.setRefresh(false);
		hSet.save();
		hSet.close();
		this.setResultSet(fSet);
	}

	private void readLocalFiles(ResFileDirSettingDbo f, DPFTInboundControlDboSet hSet) throws DPFTRuntimeException {
		DPFTFileReader reader = null;
		String filename = null;
		if (f.hasControlFile()) {
			reader = f.getLocalControlFileReader();

			filename = f.getControlFileName();
		} else {
			reader = f.getLocalFileReader();
			filename = f.getDataFileName();
		}

		if (reader.isPattern(filename)) {

			String[] flist = reader.matchPattern(filename);
			for (String fname : flist) {

				 if (reader.read(fname, f.hasControlFile())) {
					// folder contain target control file
					DPFTInboundControlDbo h = (DPFTInboundControlDbo) hSet.add();
					h.setValue("chal_name", f.getString("chal_name"));
					h.setValue("d_file", (f.hasControlFile()) ? f.getDataFileName() : fname);
					h.setValue("h_file", (f.hasControlFile()) ? fname : null);
					if (f.hasControlFile())
						h.setValue("quantity", reader.get("quantity"));
					else
						h.setValue("quantity", reader.getReadDataCount());

					h.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_RUN);
					h.setValue("process_time", DPFTUtil.getCurrentTimeStampAsString());

					// send Notification
					Object[] params = { h.getString("d_file"), h.getString("process_time") };
					// DPFTUtil.pushNotification(new DPFTMessage("CUSTOM",
					// "TFB00013I", params));
				}
			}
		} else {
			// 兩者到期才跑
//			h.setValue("d_file", f.getDataFileName());
//			h.setValue("h_file", f.getControlFileName());

			 if (reader.read(filename, f.hasControlFile())) {	
				// folder contain target control file
				DPFTInboundControlDbo h = (DPFTInboundControlDbo) hSet.add();
				h.setValue("chal_name", f.getString("chal_name"));
				h.setValue("d_file", f.getDataFileName());
				h.setValue("h_file", f.getControlFileName());
				if (f.hasControlFile())
					h.setValue("quantity", reader.get("quantity"));
				else
					h.setValue("quantity", reader.getReadDataCount());

				h.setValue("process_status", GlobalConstants.DPFT_CTRL_STAT_RUN);
				h.setValue("process_time", DPFTUtil.getCurrentTimeStampAsString());
				h.setValue("insta", f.getString("insta"));

				// send Notification
				Object[] params = { h.getString("d_file"), h.getString("process_time") };
				// DPFTUtil.pushNotification(new DPFTMessage("CUSTOM",
				// "TFB00013I", params));
			}
		}
	}

	private DPFTInboundControlDboSet getRespondControlSet() throws DPFTRuntimeException {
		try {
			return (DPFTInboundControlDboSet) this.getDBConnector().getDboSet("H_INBOUND_RES", "rownum<=1");
			// return (DPFTInboundControlDboSet) this.getDBConnector().getDboSet("TOP 1 *",
			// "H_INBOUND_RES", "");
		} catch (DPFTConnectionException e) {
			throw new DPFTActionException(this, "SYSTEM", "DPFT0003E", e);
		}
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTActionException {
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}

}

package com.ibm.tfb.ext.action;

import java.util.HashMap;
import java.util.Set;

import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicFileOutput;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.util.BsmFileFormatter;

public class InstaBsmActionDataFileOutput extends DPFTActionDataFileOutput {

	public DPFTFileFormatter getFileFormatter(DPFTDbo dPFTDbo) throws DPFTRuntimeException {
		String[] header = { "SEND_DATE", "SEND_TIME", "SYSID", "SERVICE_TYPE" };
		return new BsmFileFormatter(new DPFTFileMetaData(meta), new DPFTFileMetaData(meta, dicSet),
				TFBUtil.buildHeaderString(header, dPFTDbo, ""));
	}

	@Override
	public String getChannelName() {
		return "BIM";
	}

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
	}

	@Override
	public void action() throws DPFTRuntimeException {
		DPFTAction prev_action = this.getPreviousAction();
		HashMap<String, String> file_out_list = new HashMap<String, String>();
		HashMap<String, String> file_charset_list = new HashMap<String, String>();
		success_ftp_files.clear();
		if (prev_action instanceof DPFTActionTableWatch) {
			DPFTDboSet rs = prev_action.getResultSet();
			HashMap<String, String> serviceTypes = new HashMap<String, String>();
			for (int i = 0; i < rs.count(); i++) {
				String tServiceType = (String) rs.getDbo(i).getColumnValue("SERVICE_TYPE");
				if (serviceTypes.get(tServiceType) == null) {
					serviceTypes.put(tServiceType, tServiceType);
				}
			}
			initMetaSetting();
			Set<String> keySet = serviceTypes.keySet();
			DPFTConfig config = DPFTUtil.getSystemDBConfig();
			DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);

			for (String key : keySet) {
				DPFTLogger.info(this, "Process tServiceType..." + key);
				DPFTOutboundDboSet oBsmSet = (DPFTOutboundDboSet) connector.getDboSet("O_BIM",
						rs.getWhere() + " and SERVICE_TYPE='" + key + "'");
				oBsmSet.load();

				if (oBsmSet.count() > 0) {
					DPFTFileFormatter formatter = getFileFormatter(oBsmSet.getDbo(0));
					if (formatter == null) {
						throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0012E");
					}
					formatter.setFileEncoding(getFileEncoding());
					formatter.format(oBsmSet);
					file_out_list = formatter.getFormatFileList();
					file_charset_list = formatter.getFormatFileCharset();

					HashMap<DPFTFileFormatter, DPFTFileFTPUtil> fmtrs = getAdditionalDataFormatters();
					if (fmtrs != null) {
						DPFTLogger.info(this, "Output additional data file...");
						for (DPFTFileFormatter fmtr : fmtrs.keySet()) {
							if (fmtr == null)
								throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0012E");

							fmtr.format(oBsmSet);
							HashMap<String, String> fl = fmtr.getFormatFileList();
							HashMap<String, String> fc = fmtr.getFormatFileCharset();
							for (String k1 : fl.keySet()) {
								file_out_list.put(k1, fl.get(k1));
							}
							for (String k2 : fc.keySet()) {
								file_out_list.put(k2, fc.get(k2));
							}
						}
					}

					/* Write out files */
					doFileOut(file_out_list, file_charset_list);
					/* Compress file to .zip format if needed */
					String[] flist = null;
					if (formatter.needDataCompression())
						flist = doZipFiles(formatter.retrieveDataCompressionInfo(), formatter.getZFileName());
					else
						flist = formatter.getFiles();
					/* FTP */
					DPFTFileFTPUtil ftpUtil = getFTPUtil();
					if (ftpUtil != null) {
						String[] ctrl_list = formatter.getControlFiles();
						getFTPUtil().doFTP_Out(ctrl_list, flist);
						success_ftp_files.add(flist[0]);
					}
				}
			}
		}
		this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}
}

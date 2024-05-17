package com.ibm.dpft.engine.core.action;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundControlDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.dbo.FileDictionaryDboSet;
import com.ibm.dpft.engine.core.dbo.FileMetaDefDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDirSettingDbo;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTConnectionException;
import com.ibm.dpft.engine.core.exception.DPFTDataFormatException;
import com.ibm.dpft.engine.core.exception.DPFTFileTransferException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTEncryptUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTMessage;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public abstract class DPFTActionDataFileOutput extends DPFTAction implements DPFTFileOutputInterface {
	protected FileMetaDefDboSet meta = null;
	protected FileDictionaryDboSet dicSet = null;
	protected ArrayList<String> success_ftp_files = new ArrayList<String>();

	abstract public String getChannelName();

	@Override
	public DPFTFileFormatter getFileFormatter() throws DPFTRuntimeException {
		return new DPFTCSVFileFormatter(new DPFTFileMetaData(meta), new DPFTFileMetaData(meta, dicSet));
	}

	@Override
	public String getOutFileLocalDir() throws DPFTRuntimeException {
		return meta.getLocalDir();
	}

	@Override
	public String getOutFileRemoteDir() throws DPFTRuntimeException {
		return meta.getRemoteDir();
	}

	@Override
	public String getFileEncoding() throws DPFTRuntimeException {
		return meta.getFileEncoding();
	}

	@Override
	public void clean() throws DPFTRuntimeException {
		if (meta != null)
			meta.clear();
		if (dicSet != null)
			dicSet.clear();
		if (this.getResultSet() != null)
			this.getResultSet().clear();
		success_ftp_files.clear();
		meta = null;
		dicSet = null;
	}

	@Override
	public void action() throws DPFTRuntimeException {
		try {
			DPFTAction prev_action = this.getPreviousAction();
			HashMap<String, String> file_out_list = new HashMap<String, String>();
			HashMap<String, String> file_charset_list = new HashMap<String, String>();
			success_ftp_files.clear();
			if (prev_action instanceof DPFTActionTableWatch) {
				DPFTDboSet rs = prev_action.getResultSet();

				// if(rs.count() > 0){
				/* has data to write out */
				initMetaSetting();
				DPFTFileFormatter formatter = getFileFormatter();
				if (formatter == null)
					throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0012E");

				formatter.setFileEncoding(getFileEncoding());
				formatter.format(rs);
				file_out_list = formatter.getFormatFileList();
				file_charset_list = formatter.getFormatFileCharset();

				HashMap<DPFTFileFormatter, DPFTFileFTPUtil> fmtrs = getAdditionalDataFormatters();
				if (fmtrs != null) {
					DPFTLogger.info(this, "Output additional data file...");
					for (DPFTFileFormatter fmtr : fmtrs.keySet()) {
						if (fmtr == null)
							throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0012E");

						fmtr.format(rs);
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
				
				/* Clear personal infos ex. decrypted IDs / decrypted acct Nos */
				clearPersonalInfos(rs);

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

				/* Compress Additional files to .zip format if needed */
				if (fmtrs != null) {
					for (DPFTFileFormatter fmtr : fmtrs.keySet()) {
						String[] addi_flist = null;
						if (fmtr.needDataCompression())
							addi_flist = doZipFiles(fmtr.retrieveDataCompressionInfo(), fmtr.getZFileName());
						else
							addi_flist = fmtr.getFiles();

						/* FTP */
						DPFTFileFTPUtil addi_ftpUtil = fmtrs.get(fmtr);
						if (addi_ftpUtil != null) {
							String[] ctrl_list = fmtr.getControlFiles();
							addi_ftpUtil.doFTP_Out(ctrl_list, addi_flist);
							success_ftp_files.add(addi_flist[0]);
						}
					}
				}
			}
			// }
			this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
		} catch (Exception e) {
			DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
			ex.handleException();
		}
	}

	protected String[] doZipFiles(HashMap<String, String[]> file_compress_list, String z_file_name)
			throws DPFTRuntimeException {
		String[] flist = null;
		if (z_file_name == null)
			flist = new String[file_compress_list.size()];
		else
			flist = new String[file_compress_list.size() + 1];

		int i = 0;
		for (String zip_name : file_compress_list.keySet()) {
			flist[i] = zip_name + ".zip";
			StringBuilder sb = new StringBuilder();
			sb.append("Compressing ");
			byte[] buffer = new byte[1024];
			try {
				FileOutputStream fos = new FileOutputStream(getOutFileLocalDir() + File.separator + flist[i]);
				ZipOutputStream zos = new ZipOutputStream(fos);
				for (String filename : file_compress_list.get(zip_name)) {
					sb.append(", ").append(filename);
					ZipEntry ze = new ZipEntry(filename);
					zos.putNextEntry(ze);
					FileInputStream in = new FileInputStream(getOutFileLocalDir() + File.separator + filename);
					int len;
					while ((len = in.read(buffer)) > 0) {
						zos.write(buffer, 0, len);
					}
					in.close();
				}
				zos.closeEntry();
				zos.close();
			} catch (Exception ex) {
				Object[] params = { flist[i] };
				throw new DPFTFileTransferException("SYSTEM", "DPFT0014E", params);
			}
			sb.append(" to Zip File :").append(zip_name);
			i++;
		}
		// Add Z file to FTP File list
		if (z_file_name != null)
			flist[i] = z_file_name;

		return flist;
	}

	@Override
	public FTPConfig getFTPConfig() {
		FTPConfig config = new FTPConfig();
		config.setHost(DPFTEngine.getSystemProperties("ftp." + this.getChannelName() + ".host"));
		config.setPort(Integer.parseInt(DPFTEngine.getSystemProperties("ftp." + this.getChannelName() + ".port")));
		config.setUser(DPFTEngine.getSystemProperties("ftp." + this.getChannelName() + ".user"));
		if (!DPFTEngine.getSystemProperties("ftp." + this.getChannelName() + ".zippassword").isEmpty()) {
			config.setZipPassword(DPFTEngine.getSystemProperties("ftp." + this.getChannelName() + ".zippassword"));
		}
		String pass = DPFTEncryptUtil.getDecryptor()
				.decrypt((DPFTEngine.getSystemProperties("ftp." + this.getChannelName() + ".password")));
		// String pass = DPFTEngine.getSystemProperties("ftp." + this.getChannelName() +
		// ".password");
		DPFTLogger.info(this, "pass:" + pass);
		config.setPassword(pass);
		return config;
	}

	protected void initMetaSetting() throws DPFTRuntimeException {

		DPFTConnector connector;
		try {
			connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig());
			meta = (FileMetaDefDboSet) connector.getDboSet("DPFT_FILE_META_DEF",
					"chal_name='" + getChannelName() + "' and active=1");
			dicSet = (FileDictionaryDboSet) connector.getDboSet("DPFT_FILE_DIC",
					"chal_name='" + getChannelName() + "' and active=1");
		} catch (DPFTConnectionException e) {
			throw new DPFTActionException(this, "SYSTEM", "DPFT0001E", e);
		}
		meta.load();
		dicSet.load();
		meta.close();
		dicSet.close();
	}

	protected void doFileOut(HashMap<String, String> file_out_list, HashMap<String, String> file_charset_list)
			throws DPFTRuntimeException {

		for (String filename : file_out_list.keySet()) {
			DPFTLogger.info(this, "filename:" + filename + " encoding:" + file_charset_list.get(filename));
			try {
				File fdir = new File(getOutFileLocalDir());
				DPFTLogger.info(this, "111");
				if (!fdir.exists()) {
					fdir.mkdirs();
				}
				DPFTLogger.info(this, "222");
				File f = new File(getOutFileLocalDir() + File.separator + filename);
				if (("UTF-8BOM").equals(file_charset_list.get(filename))) {
					Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF-8"));
					out.write('\ufeff');
					out.write(file_out_list.get(filename));
					out.flush();
					out.close();
				} else {
					Writer out = new BufferedWriter(
							new OutputStreamWriter(new FileOutputStream(f), file_charset_list.get(filename)));
					out.write(file_out_list.get(filename));
					out.flush();
					out.close();
				}
				DPFTLogger.info(this, "333");

			} catch (Exception e) {
				e.printStackTrace();
				Object[] params = { filename };
				DPFTLogger.error(this, e.toString());
				throw new DPFTDataFormatException("SYSTEM", "DPFT0015E", params);
			}
		}
	}
	
	protected void clearPersonalInfos(DPFTDboSet rs) {
		try {
			if ( getChannelName() == "APP" ) {
				for (int i=0; i<rs.count(); i++) {
					rs.getDbo(i).setValue("RECEIVER", "");
					rs.getDbo(i).setValue("EXTRAMSG", "");
				}
			}
			else if ( getChannelName() == "CTI" ) {
				for (int i=0; i<rs.count(); i++) {
					rs.getDbo(i).setValue("DECRYPTED_CUSTOMER_ID", "");
					rs.getDbo(i).setValue("TEMP_FIELD13", "");
					rs.getDbo(i).setValue("CUSTOMER_ACCT_NO", "");
				}
			}
			else if ( getChannelName() == "CSM" || getChannelName() == "DCS" ) {
				for (int i=0; i<rs.count(); i++) {
					rs.getDbo(i).setValue("LONG_CUSTOMER_ID", "");
					rs.getDbo(i).setValue("SHORT_CUSTOMER_ID", "");
				}
			}
			else if ( getChannelName() == "EFL" || getChannelName() == "LIN" || getChannelName() == "MBA" || getChannelName() == "RMB" ) {
				for (int i=0; i<rs.count(); i++) {
					rs.getDbo(i).setValue("DECRYPTED_CUSTOMER_ID", "");
				}
			}
			rs.setRefresh(false);
			rs.save();
			rs.close();
			
		} catch (DPFTRuntimeException e) {
			DPFTLogger.debug(this, "Error when clearing personal info from channel " + getChannelName());
			DPFTLogger.error(this, e.toString());
			e.printStackTrace();
		}
	}

	@Override
	public void finish() throws DPFTRuntimeException {
		/* update Obnd Control Table Records */
		DPFTLogger.info(this, "Updating Outbound Control Records...");
		DPFTOutboundDboSet rSet = (DPFTOutboundDboSet) this.getPreviousAction().getResultSet();
		DPFTOutboundControlDboSet ctrlSet = rSet
				.getControlTableRecords("process_status='" + GlobalConstants.DPFT_OBND_STAT_RUN + "'");
		ctrlSet.taskComplete();
		ctrlSet.close();

		// send Notification
		String cmp_code = ctrlSet.getDbo(0).getString("camp_code");
		for (String sf : success_ftp_files) {
			Object[] params = { sf, this.getOutFileRemoteDir() };
			// DPFTUtil.pushNotification(DPFTUtil.getCampaignOwnerEmail(cmp_code),
			// new DPFTMessage("CUSTOM", "TFB00011I", params));
		}
		success_ftp_files.clear();
	}

	public String getFTPType(String chal_name) {
		String ftptype = DPFTEngine.getSystemProperties("ftp." + chal_name + ".ftptype");
		if (!StringUtils.isEmpty(ftptype)) {
			switch (DPFTEngine.getSystemProperties("ftp." + chal_name + ".ftptype")) {
			case "FTP":
				return "com.ibm.dpft.engine.core.util.DPFTBasicFTPUtil";
			case "FTPS":
				return "com.ibm.dpft.engine.core.util.DPFTFTPSUtil";
			case "SFTP":
				return "com.ibm.dpft.engine.core.util.DPFTSFTPUtil";
			default:
				return "com.ibm.dpft.engine.core.util.DPFTBasicFTPUtil";
			}
		} else {
			return "";
		}
	}

	@Override
	public DPFTFileFTPUtil getFTPUtil() throws DPFTRuntimeException {
		DPFTFileFTPUtil ftputil = null;
		try {
			ftputil = (DPFTFileFTPUtil) Class.forName(getFTPType(getChannelName()))
					.getConstructor(String.class, String.class, FTPConfig.class)
					.newInstance(getOutFileLocalDir(), getOutFileRemoteDir(), getFTPConfig());
		} catch (Exception e) {

			DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
			ex.handleException();
		}
		return ftputil;
	}

	@Override
	public void handleException(DPFTActionException e) throws DPFTRuntimeException {
		DPFTOutboundDboSet rSet = (DPFTOutboundDboSet) this.getPreviousAction().getResultSet();
		DPFTOutboundControlDboSet ctrlSet = rSet
				.getControlTableRecords("process_status='" + GlobalConstants.DPFT_OBND_STAT_RUN + "'");
		ctrlSet.error();
		ctrlSet.close();
		throw e;
	}
}

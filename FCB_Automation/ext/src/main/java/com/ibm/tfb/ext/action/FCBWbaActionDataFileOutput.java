package com.ibm.tfb.ext.action;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.action.DPFTActionDataFileOutput;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTBasicFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFTPUtil;
import com.ibm.dpft.engine.core.util.DPFTFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.tfb.ext.util.FCBLinFileFormatter;
import com.ibm.tfb.ext.util.FCBWbaFileFormatter;


import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.CompressionLevel;
import net.lingala.zip4j.model.enums.CompressionMethod;
import net.lingala.zip4j.model.enums.EncryptionMethod;


public class FCBWbaActionDataFileOutput extends DPFTActionDataFileOutput {

	@Override
	public DPFTFileFormatter getFileFormatter() throws DPFTRuntimeException {
		return new FCBWbaFileFormatter(new DPFTFileMetaData(meta), new DPFTFileMetaData(meta, dicSet));
	}
	
	@Override
	public String getChannelName() {
		return "WBA";
	}

	@Override
	public HashMap<DPFTFileFormatter, DPFTFileFTPUtil> getAdditionalDataFormatters() {
		return null;
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

				/* Compress file to .zip format if needed */
				String[] flist = null;
				if (formatter.needDataCompression())
					flist = doZipFiles(formatter.retrieveDataCompressionInfo(), formatter.getZFileName());
				else
					flist = formatter.getFiles();
				
				DPFTFileFTPUtil ftpUtil = getFTPUtil();
				String zippassword = ftpUtil.getConfig().getZipPassword();
				DPFTLogger.info(this, "zipPassword:" + zippassword);
				
				for ( int i = 0; i < flist.length; i++) {
					ArrayList<File> list = new ArrayList<>();
					String filename = flist[i];
					File localfile = new File(ftpUtil.getLocalDir() + "/" + filename);
					String h_filename = formatter.getControlFileName();
					File h_localfile = new File(ftpUtil.getLocalDir() + "/" + h_filename);
					list.add(localfile);
					list.add(h_localfile);
					String newFileName = filename.replace(".D","") + ".zip";
					ZipFile zipFile = new ZipFile(ftpUtil.getLocalDir() + "/" + newFileName,zippassword.toCharArray());
//					ZipParameters parameters = new ZipParameters();
//					parameters.setCompressionMethod(CompressionMethod.COMP_DEFLATE);
//					parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
//					parameters.setEncryptFiles(true);
//					parameters.setEncryptionMethod(Zip4jConstants.ENC_METHOD_STANDARD);
//					parameters.setPassword(zippassword);


					// 創建 ZipParameters 對象並設置參數
					ZipParameters parameters = new ZipParameters();
					parameters.setCompressionMethod(CompressionMethod.DEFLATE); // 設置壓縮方法為DEFLATE
					parameters.setCompressionLevel(CompressionLevel.NORMAL); // 設置壓縮等級為NORMAL

// 設置加密參數
					parameters.setEncryptFiles(true); // 啟用加密
					parameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD); // 設置加密方法為ZIP標準



					try {
						zipFile.addFiles(list, parameters);
					} catch (ZipException e) {
						e.printStackTrace();
					}
					
					flist[i] = newFileName;
					DPFTLogger.info(this, "zipFileName:" + flist[i]);
				}

				/* FTP */
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

}

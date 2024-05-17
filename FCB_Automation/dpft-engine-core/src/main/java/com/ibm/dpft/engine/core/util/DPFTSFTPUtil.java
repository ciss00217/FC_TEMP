package com.ibm.dpft.engine.core.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Duration;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTSFTPUtil extends DPFTFileFTPUtil {
	private static final Object lock = new Object();

	public DPFTSFTPUtil(String ldir, String rdir, FTPConfig cfg) {
		super(ldir, rdir, cfg);
	}

	public static String createConnectionString(String hostName, int port, String username, String password,
			String remoteFilePath) throws UnsupportedEncodingException {
		// result: "sftp://user:123456@domainname.com:port/resume.pdf
		
		String sftpUri = "sftp://" + URLEncoder.encode(username, "UTF-8") + ":" + URLEncoder.encode(password, "UTF-8")
				+ "@" + hostName + "/" + remoteFilePath;
		DPFTLogger.info(DPFTSFTPUtil.class, "sftpUri:" + sftpUri);
		return sftpUri;
	}

	public static FileSystemOptions createDefaultOptions() throws FileSystemException {
		// Create SFTP options
		FileSystemOptions opts = new FileSystemOptions();

		// SSH Key checking
		SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");
		SftpFileSystemConfigBuilder.getInstance().setIdentities(opts, new File[0]);

		// Root directory set to user home
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);

		// Timeout is count by Milliseconds
		SftpFileSystemConfigBuilder.getInstance().setSessionTimeout(opts, Duration.ofSeconds(10));
		SftpFileSystemConfigBuilder.getInstance().setConnectTimeout(opts,  Duration.ofSeconds(10));

		return opts;
	}

	@Override
	public void doFTP_Out(String[] c_out_list, String[] file_out_list) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTLogger.info(this, "Initializing SFTPOut Process...");
			DPFTLogger.info(this, "Ready to transfer Data files...");
			outFTP(file_out_list);
			DPFTLogger.info(this, "Executing SFTPOut Command from bash...");
			outFTP(c_out_list);
			DPFTLogger.info(this, "Ready to transfer Control files...");
			DPFTLogger.info(this, "Executing SFTPOut...");
		}
	}

	public void outFTP(String[] file_out_list) throws DPFTRuntimeException {
		if (file_out_list == null || file_out_list.length == 0) {
			return;
		}
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();
			FTPConfig config = getConfig();
			/* Read local files */
			for (String filename : file_out_list) {

				File f = new File(getLocalDir() + "/" + filename);
				if (!f.exists()) {
					throw new RuntimeException("Error. Local file not found");
				}
				String remoteDir = getRemoteDir() + filename;
				DPFTLogger.info(this, "Start upload FTP file:" + filename + " to Remote Dir:" + remoteDir);
				// Create local file object
				FileObject localFile = manager.resolveFile(f.getAbsolutePath());
				// Create remote file object
				FileObject remoteFile = manager.resolveFile(createConnectionString(config.getHost(), config.getPort(),
						config.getUser(), config.getPassword(), remoteDir), createDefaultOptions());
				long time_start = System.currentTimeMillis();
				// Copy local file to sftp server
				remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
				long time_end = System.currentTimeMillis();
				DPFTLogger.info(this, "Successfully upload FTP file:" + filename + "to Remote Dir:" + remoteDir
						+ " Time Spent: " + String.format("%.3f", (float) ((time_end - time_start) / 1000)) + " Sec.");
			}
		} catch (Exception e) {
			throw new DPFTRuntimeException("SYSTEM", "DPFT0003E", e);
		} finally {
			manager.close();
		}
	}

	@Override
	public int doFTP_Get(String[] c_list, String[] f_list) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTLogger.info(this, "Initializing SFTPGet Process...");
			DPFTLogger.info(this, "Ready to transfer Data files...");
			DPFTLogger.info(this, "Executing SFTPGet ...");
			int rtnCode2 = getFTP(f_list);
			DPFTLogger.info(this, f_list + " : " + rtnCode2);
			DPFTLogger.info(this, "Ready to transfer Control files...");
			DPFTLogger.info(this, "Executing SFTPGet ...");
			int rtnCode1 = getFTP(c_list);
			DPFTLogger.info(this, c_list + " : " + rtnCode1);
			if (rtnCode1 == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS
					&& rtnCode2 == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS) {
				if (isDataFileExist(f_list)) {
					return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;

				} else {
					return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
				}
			} else {
				return GlobalConstants.ERROR_LEVEL_TRF_FAILURE;
			}
		}
	}

	synchronized public int getFTP(String[] getfileList) throws DPFTRuntimeException {
		if (getfileList == null || getfileList.length == 0) {
			return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
		}
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();
			FTPConfig config = getConfig();

			/* Read local files */
			for (String filename : getfileList) {
				String localfile = getLocalDir() + "/" + filename;
				File fdir = new File(getLocalDir());
				if (!fdir.exists()) {
					fdir.mkdirs();
				}
				String remoteDir = getRemoteDir() + "/" + filename;
				DPFTLogger.info(this, "Start download FTP file:" + remoteDir + " to Local Dir:" + localfile);

				
	//tart download FTP file:APP/APP_RESP_20240324.D to Local Dir:D:\IBM\DPFT_Runtime_dev\response\APP/APP_RESP_20240324.D
				
				// Create local file object
				FileObject localFile = manager.resolveFile(localfile);

				// Create remote file object
				FileObject remoteFile = manager.resolveFile(createConnectionString(config.getHost(), config.getPort(),
						config.getUser(), config.getPassword(), remoteDir), createDefaultOptions());
				if (remoteFile.exists()) {
					long time_start = System.currentTimeMillis();
					// Copy local file to sftp server
					localFile.copyFrom(remoteFile, Selectors.SELECT_SELF);

					long time_end = System.currentTimeMillis();
					DPFTLogger.info(this,
							"Successfully download FTP file:" + remoteDir + "to Local Dir:" + remoteDir
									+ " Time Spent: " + String.format("%.3f", (float) ((time_end - time_start) / 1000))
									+ " Sec.");
				} else {
					DPFTLogger.error(this, "FTP retrieveFile not exists");
					return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
				}
			}

		} catch (Exception e) {
			throw new DPFTRuntimeException("SYSTEM", "DPFT0003E", e);
//			return GlobalConstants.ERROR_LEVEL_TRF_FAILURE;
		} 
			manager.close();
		
		return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
	}

	private boolean isDataFileExist(String[] d_file_list) {
		boolean rtnV = true;
		String ldir = this.getLocalDir();
		for (String filename : d_file_list) {
			File f = new File(ldir + File.separator + filename);
			if (!f.exists())
				rtnV = false;
		}
		return rtnV;
	}

	@Override
	public void doFTP_Del(String[] del_clist, String[] del_flist) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTLogger.info(this, "Initializing SFTPDelete Process...");
			DPFTLogger.info(this, "Get File Success, removing files from remote server...");
			deleteFTP(del_flist);
			deleteFTP(del_clist);
			DPFTLogger.info(this, "Executing SFTPDelete...");
		}
	}

	synchronized public int deleteFTP(String[] deletefileList) {
		if (deletefileList == null || deletefileList.length == 0) {
			return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
		}

		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();
			FTPConfig config = getConfig();

			/* Read local files */
			for (String filename : deletefileList) {
				String remoteDir = getRemoteDir() + "/" + filename;
				FileObject remoteFile = manager.resolveFile(createConnectionString(config.getHost(), config.getPort(),
						config.getUser(), config.getPassword(), remoteDir), createDefaultOptions());
				if (remoteFile.exists()) {
					remoteFile.delete();
					DPFTLogger.info(this, "Successfully Delete FTP file:" + filename);
				}
			}

		} catch (Exception e) {
			DPFTLogger.error(this, "Error when deleteFTP Files :", e);
			return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
		} finally {
			manager.close();
		}
		return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
	}

	@Override
	public void doFTP_Move(String[] clist, String remoteDir) throws DPFTRuntimeException {
		// TODO Auto-generated method stub
		
	}
	
public static void main(String args[]) throws DPFTRuntimeException, FileSystemException, UnsupportedEncodingException{
		
	StandardFileSystemManager manager = new StandardFileSystemManager();
	manager.init();

	FTPConfig config = new FTPConfig();
	
	String host="10.14.88.90";
	String post="22";
	String user="ftpuser";
	String pass=  DPFTEncryptUtil.getDecryptor()
			.decrypt(("RZwQw6p7/dbA3JJ94ZwjqBkfpw2yxqVT"));
	config.setHost(host);
	config.setPort(Integer.parseInt(post));
	config.setUser(user);
	config.setPassword(pass);
	

	String remoteDir = "APP" + "/" + "APP_RESP_20240324.D";
	String localFile = "D:\\IBM\\DPFT_Runtime_dev\\response\\APP/APP_RESP_20240324.D";
	
	FileObject remoteFile = manager.resolveFile("sftp://ftpuser:Fcbfcb@11111@10.14.88.90/APP/APP_RESP_20240324.D");
	FileObject localObjFile = manager.resolveFile(localFile);

//	localObjFile.copyFrom(remoteFile, Selectors.SELECT_SELF);
	
	}
}

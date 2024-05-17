package com.ibm.dpft.engine.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Arrays;
import java.util.Locale;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPSClient;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.FTPConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTFTPSUtil extends DPFTFileFTPUtil {
	
	private static final Object lock = new Object();

	public DPFTFTPSUtil(String ldir, String rdir, FTPConfig cfg) {
		super(ldir, rdir, cfg);
	}

	@Override
	public void doFTP_Out(String[] c_out_list, String[] file_out_list) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTLogger.info(this, "Initializing FTPSOut Process...");
			DPFTLogger.info(this, "Ready to transfer Data files...");
			outFTP(file_out_list);
			DPFTLogger.info(this, "Executing FTPSOut Command from bash...");
			outFTP(c_out_list);
			DPFTLogger.info(this, "Ready to transfer Control files...");
			DPFTLogger.info(this, "Executing FTPSOut...");
		}
	}

	synchronized public void outFTP(String[] file_out_list) throws DPFTRuntimeException {
		if (file_out_list == null || file_out_list.length == 0) {
			return;
		}
		FTPSClient ftp_client = new SSLSessionReuseFTPSClient("TLSv1.2",false);
		
		try {
			FTPConfig config = getConfig();
			
			DPFTLogger.info(this, "@@@@@@@@start connect FTPS "+Arrays.toString(file_out_list));
			
			ftp_client.connect(config.getHost(), config.getPort());
			
			DPFTLogger.info(this, "@@@@@@@@end to connect FTPS "+Arrays.toString(file_out_list));

			if (!ftp_client.login(config.getUser(), config.getPassword())) {
				ftp_client.logout();
			}
			ftp_client.enterLocalPassiveMode();
			ftp_client.setFileType(FTP.BINARY_FILE_TYPE);
			
			/*
			 *  20181005 排除無法傳送大於10K的名單檔
			 *  1.execPBSZ:變更設定保護緩衝區大小
			 *  2.execPROT:設定Data Channel 為"P(Private)" 
			 */
			ftp_client.execPBSZ(0);
			ftp_client.execPROT("P");
			
			String remoteDir = getRemoteDir();
//			ftp_client.setControlEncoding("utf8");
			boolean directoryExists = ftp_client.changeWorkingDirectory(new String(remoteDir.getBytes(), "8859_1"));
			if (!directoryExists) {
				ftp_client.makeDirectory(remoteDir);
			}
			ftp_client.changeWorkingDirectory(remoteDir);
			/* Read local files */
			for (String filename : file_out_list) {
				File localfile = new File(getLocalDir() + "/" + filename);
				InputStream in = new FileInputStream(localfile);

				DPFTLogger.info(this,
						"Start upload FTP file:" + localfile + " to Remote Dir:" + remoteDir + "/" + filename);

				long time_start = System.currentTimeMillis();
				boolean done = ftp_client.storeFile(filename, in);
				long time_end = System.currentTimeMillis();
				in.close();
				if (done) {
					DPFTLogger.info(this,
							"Successfully upload FTP file:" + filename + "to Remote Dir:" + remoteDir + "/" + filename
									+ " Time Spent: " + String.format("%.3f", (float) ((time_end - time_start) / 1000))
									+ " Sec.");
				} else {
					DPFTLogger.error(this, "FTP upload Error:" + ftp_client.getReplyString());
				}
			}
			ftp_client.logout();
		} catch (Exception e) {
			DPFTLogger.error(this, "Error when upload FTPS Files :", e);
			throw new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
		} finally {
			try {
				if (ftp_client.isConnected()) {
					ftp_client.disconnect();
				}
			} catch (IOException ex) {
				DPFTLogger.error(this, "Error when disconnecting FTP Server :", ex);
			}
		}
	}

	@Override
	public int doFTP_Get(String[] c_list, String[] f_list) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTLogger.info(this, "Initializing FTPSGet Process...");
			DPFTLogger.info(this, "Ready to transfer Data files...");
			DPFTLogger.info(this, "Executing FTPSGet ...");
			int rtnCode2 = getFTP(f_list);
			DPFTLogger.info(this, f_list + " : " + rtnCode2);
			DPFTLogger.info(this, "Ready to transfer Control files...");
			DPFTLogger.info(this, "Executing FTPSGet ...");
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

	@Override
	public void doFTP_Del(String[] del_clist, String[] del_flist) throws DPFTRuntimeException {
		synchronized (lock) {
			DPFTLogger.info(this, "Initializing FTPSDelete Process...");
			DPFTLogger.info(this, "Get File Success, removing files from remote server...");
			deleteFTP(del_flist);
			deleteFTP(del_clist);
			DPFTLogger.info(this, "Executing FTPSDelete...");
		}
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

	synchronized public int getFTP(String[] getfileList) throws DPFTRuntimeException {
		if (getfileList == null || getfileList.length == 0) {
			return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
		}

		FTPSClient ftp_client = new SSLSessionReuseFTPSClient("TLSv1.2",false);
	
		
		try {
			FTPConfig config = getConfig();
			// 534 error
			//ftp_client.setFileType(FTP.BINARY_FILE_TYPE);
//			ftp_client.execPBSZ(0);
//			ftp_client.execPROT("P");
			ftp_client.connect(config.getHost(), config.getPort());
			ftp_client.enterLocalPassiveMode();
			if (!ftp_client.login(config.getUser(), config.getPassword())) {
				ftp_client.logout();
				DPFTLogger.error(this, "FTP Login Error");
				return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
			}

			ftp_client.execPBSZ(0);
			ftp_client.execPROT("P");
			ftp_client.setFileType(FTP.BINARY_FILE_TYPE);

			/* Read local files */
			for (String filename : getfileList) {
				String localfile = getLocalDir() + "/" + filename;
				File fdir = new File(getLocalDir());
				if (!fdir.exists()) {
					fdir.mkdirs();
				}
				String remoteDir = getRemoteDir() + "/" + filename;
				DPFTLogger.info(this, "Start FTP file:" + remoteDir + " to Local Dir:" + localfile);
				long time_start = System.currentTimeMillis();
				FTPFile[] files = ftp_client.listFiles(remoteDir);
				if (files.length > 0) {
					FileOutputStream output = new FileOutputStream(localfile);
					boolean downloadSuccess = ftp_client.retrieveFile(remoteDir, output);
					DPFTLogger.info(this, "downloadSuccess:" + downloadSuccess);
					if (downloadSuccess) {
						long time_end = System.currentTimeMillis();
						DPFTLogger.info(this,
								"Successfully download FTP file:" + remoteDir + "to Local Dir:" + remoteDir
										+ " Time Spent: "
										+ String.format("%.3f", (float) ((time_end - time_start) / 1000)) + " Sec.");
					} else {
						DPFTLogger.error(this, "FTP retrieveFile Error:" + ftp_client.getReplyCode());
						return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
					}
					output.close();
				} else {
					DPFTLogger.error(this, "FTP file not exits.");
					return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
				}
			}
			ftp_client.logout();
		} catch (Exception e) {
			DPFTLogger.error(this, "Error getFTP Files :", e);
			return GlobalConstants.ERROR_LEVEL_TRF_FAILURE;
		} finally {
			if (ftp_client.isConnected()) {
				try {
					ftp_client.disconnect();
				} catch (IOException e) {
					DPFTLogger.error(this, "Error when disconnecting FTP Server :", e);
				}
			}
		}

		return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
	}

	synchronized public int deleteFTP(String[] deletefileList) {
		if (deletefileList == null || deletefileList.length == 0) {
			return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
		}

		FTPSClient ftp_client = new SSLSessionReuseFTPSClient("TLSv1.2",false);
		try {
			FTPConfig config = getConfig();
			ftp_client.connect(config.getHost(), config.getPort());
			if (!ftp_client.login(config.getUser(), config.getPassword())) {
				ftp_client.logout();
				DPFTLogger.error(this, "FTP Login Error");
				return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
			}

			ftp_client.enterLocalPassiveMode();
			ftp_client.setFileType(FTP.BINARY_FILE_TYPE);

			/* Read local files */
			for (String filename : deletefileList) {
				String remoteDir = getRemoteDir() + "/" + filename;
				boolean deleteSuccess = ftp_client.deleteFile(remoteDir);
				DPFTLogger.info(this, "deleteSuccess:" + deleteSuccess);
				if (deleteSuccess) {
					DPFTLogger.info(this, "Successfully Delete FTP file:" + filename);
				} else {
					DPFTLogger.error(this, "FTP deleteFile Error:" + ftp_client.getReplyCode());
					return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
				}
			}
			ftp_client.logout();
		} catch (Exception e) {
			DPFTLogger.error(this, "Error when deleteFTP Files :", e);
			return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
		} finally {
			try {
				if (ftp_client.isConnected()) {

					ftp_client.disconnect();
				}
			} catch (IOException ex) {
				DPFTLogger.error(this, "Error when disconnecting FTP Server :", ex);
			}
		}
		return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
	}

	@Override
	public void doFTP_Move(String[] clist, String remoteDir) throws DPFTRuntimeException {
		// TODO Auto-generated method stub
		
	}
	
	public static class SSLSessionReuseFTPSClient extends FTPSClient {

	    public SSLSessionReuseFTPSClient(final String protocol, final boolean isImplicit) {
	    	super(protocol, isImplicit);
	    
	    }
//	    // adapted from:
//	    // https://trac.cyberduck.io/browser/trunk/ftp/src/main/java/ch/cyberduck/core/ftp/FTPClient.java
//	    @Override
//	    protected void _prepareDataSocket_(final Socket socket) throws IOException {
//	        if (socket instanceof SSLSocket) {
//	            // Control socket is SSL
//	            final SSLSession session = ((SSLSocket) _socket_).getSession();
//	            if (session.isValid()) {
//	                final SSLSessionContext context = session.getSessionContext();
//	                try {
//	                	
//	                	
//	                	System.out.println( context.getClass().getPackage());
//	                	System.out.println( context.getClass().getSimpleName());
//
//	                	System.out.println( context.getClass().getName());
//	                	System.out.println( context.getClass().getName());
//
//	                	
//	                    final Field sessionHostPortCache = context.getClass().getDeclaredField("sessionHostPortCache");
//	                    sessionHostPortCache.setAccessible(true);
//	                    final Object cache = sessionHostPortCache.get(context);
//	                    final Method method = cache.getClass().getDeclaredMethod("put", Object.class, Object.class);
//	                    method.setAccessible(true);
//	                    method.invoke(cache, String
//	                            .format("%s:%s", socket.getInetAddress().getHostName(), String.valueOf(socket.getPort()))
//	                            .toLowerCase(Locale.ROOT), session);
//	                    method.invoke(cache, String
//	                            .format("%s:%s", socket.getInetAddress().getHostAddress(), String.valueOf(socket.getPort()))
//	                            .toLowerCase(Locale.ROOT), session);
//	                } catch (NoSuchFieldException e) {
//	                    throw new IOException(e);
//	                } catch (Exception e) {
//	                    throw new IOException(e);
//	                }
//	            } else {
//	                throw new IOException("Invalid SSL Session");
//	            }
//	        }
//	    }
	}

}

package com.ibm.dpft.engine.core.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTFileTransferException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTcdFTPUtil extends DPFTFileFTPUtil {
	private String cd_ftp_bash_cmd = null;
	private ArrayList<String> h_file_list = new ArrayList<String>();
	private ArrayList<String> d_file_list = new ArrayList<String>();
	private String ftp_cmd_file_name = null;
	private String ftp_logfile = null;
	private String profile = null;
	private long timestamp;
	private static final Object lock = new Object();

	public DPFTcdFTPUtil(String ldir, String rdir, String profile) {
		super(ldir, rdir, null);
		this.profile = profile;
		cd_ftp_bash_cmd = DPFTEngine.getSystemProperties(profile + ".bash.root") + " -cpa:%s -cpw:%s -c:%s -e -s:%s";
	}
	
	@Override
	public int doFTP_Get(String[] c_in_list, String[] file_in_list) throws DPFTRuntimeException {
		synchronized(lock){
			DPFTLogger.info(this, "Initializing cdFTP+ Process...");
			initFTP(c_in_list, file_in_list);
			DPFTLogger.info(this, "Ready to transfer Data files...");
			get_D_File_OUT_FTPCmd2LocalDir();
			DPFTLogger.info(this, "Executing cdFTP+ Command from bash...");
			int rtnCode1 = execCdFTP();
			DPFTLogger.info(this, "Ready to transfer Data files...");
			get_H_File_OUT_FTPCmd2LocalDir();
			DPFTLogger.info(this, "Executing cdFTP+ Command from bash...");
			int rtnCode2 = execCdFTP();
			if(rtnCode1 == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS && rtnCode2 == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS){
				if(isDataFileExist())
					return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
				else
					return GlobalConstants.ERROR_LEVEL_READ_FAILURE;
			}else{
				return GlobalConstants.ERROR_LEVEL_TRF_FAILURE;
			}
		}
	}

	private boolean isDataFileExist() {
		boolean rtnV = true;
		String ldir = this.getLocalDir();
		for(String filename: d_file_list){
			File f = new File(ldir + File.separator + filename);
			if(!f.exists())
				rtnV = false;
		}
		return rtnV;
	}

	public void unlock() {
		//Remove .lock file
		File outfile = new File(getLocalDir() + File.separator + "lock");
		if(outfile.delete()){
			DPFTLogger.debug(this, "Unlock Local Directory : " + getLocalDir());
		}
	}

	public void lock() throws DPFTRuntimeException {
		//Write .lock file to local Directory to prevent other thread from modifying contents
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if(!fdir.exists()){
			fdir.mkdirs();
		}
		
		try{
			File outfile = new File(ldir + File.separator + "lock");
			Writer out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(outfile), "UTF-8"));
			out.write("");
			out.flush();
			out.close();
			DPFTLogger.debug(this, "lock Local Directory : " + getLocalDir());
		}catch(Exception e){
			Object[] params = {ldir};
			throw new DPFTFileTransferException("SYSTEM", "DPFT0020E", params);
		}
	}

	private void del_Remote_File_OUT_FTPCmd2LocalDir() throws DPFTRuntimeException {
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if(!fdir.exists()){
			fdir.mkdirs();
		}
		
		DPFTPCmdBuilder cb = new DPFTPCmdBuilder(GlobalConstants.FTP_MODE_BINARY);
		cb.cd(getRemoteDir());
		cb.prompt(false);
		cb.delete(d_file_list.toArray(new String[d_file_list.size()]));
		cb.delete(h_file_list.toArray(new String[h_file_list.size()]));
		cb.quit();
		
		if(cb.hasFTPCmd()){
			try{
				ftp_cmd_file_name = ldir + File.separator + "cmd" + File.separator + "del_" + timestamp + ".FTP";
				ftp_logfile = ldir + File.separator + "log" + File.separator + "del_" + timestamp;
				File outfile = new File(ftp_cmd_file_name);
				if(!outfile.getParentFile().exists())
					outfile.getParentFile().mkdirs();
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outfile), "UTF-8"));
				out.write(cb.toString());
				out.flush();
				out.close();
			}catch(Exception e){
				throw new DPFTFileTransferException("SYSTEM", "DPFT0019E", e);
			}
		}
	}

	private void get_H_File_OUT_FTPCmd2LocalDir() throws DPFTRuntimeException {
		if(h_file_list.isEmpty())
			return;
		
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if(!fdir.exists()){
			fdir.mkdirs();
		}
		
		DPFTPCmdBuilder cb = new DPFTPCmdBuilder(GlobalConstants.FTP_MODE_BINARY);
		cb.cd(getRemoteDir());
		cb.lcd(ldir);
		cb.prompt(false);
		cb.mget(h_file_list.toArray(new String[h_file_list.size()]));
		cb.quit();
		
		if(cb.hasFTPCmd()){
			try{
				ftp_cmd_file_name = ldir + File.separator + "cmd" + File.separator + "hget_" + timestamp + ".FTP";
				ftp_logfile = ldir + File.separator + "log" + File.separator + "hget_" + timestamp;
				File outfile = new File(ftp_cmd_file_name);
				if(!outfile.getParentFile().exists())
					outfile.getParentFile().mkdirs();
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outfile), "UTF-8"));
				out.write(cb.toString());
				out.flush();
				out.close();
			}catch(Exception e){
				throw new DPFTFileTransferException("SYSTEM", "DPFT0019E", e);
			}
		}
	}

	private void get_D_File_OUT_FTPCmd2LocalDir() throws DPFTRuntimeException {
		if(d_file_list.isEmpty())
			return;
		
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if(!fdir.exists()){
			fdir.mkdirs();
		}
		
		DPFTPCmdBuilder cb = new DPFTPCmdBuilder(GlobalConstants.FTP_MODE_BINARY);
		cb.cd(getRemoteDir());
		cb.lcd(ldir);
		cb.prompt(false);
		cb.mget(d_file_list.toArray(new String[d_file_list.size()]));
		cb.quit();
		
		if(cb.hasFTPCmd()){
			try{
				ftp_cmd_file_name = ldir + File.separator + "cmd" + File.separator + "dget_" + timestamp + ".FTP";
				ftp_logfile = ldir + File.separator + "log" + File.separator + "dget_" + timestamp;
				File outfile = new File(ftp_cmd_file_name);
				if(!outfile.getParentFile().exists())
					outfile.getParentFile().mkdirs();
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outfile), "UTF-8"));
				out.write(cb.toString());
				out.flush();
				out.close();
			}catch(Exception e){
				throw new DPFTFileTransferException("SYSTEM", "DPFT0017E", e);
			}
		}
	}

	@Override
	public void doFTP_Out(String[] c_out_list, String[] file_out_list) throws DPFTRuntimeException {
		synchronized(lock){
			DPFTLogger.info(this, "Initializing cdFTP+ Process...");
			initFTP(c_out_list, file_out_list);
			DPFTLogger.info(this, "Ready to transfer Data files...");
			write_D_File_OUT_FTPCmd2LocalDir();
			DPFTLogger.info(this, "Executing cdFTP+ Command from bash...");
			execCdFTP();
			DPFTLogger.info(this, "Ready to transfer Control files...");
			write_H_File_OUT_FTPCmd2LocalDir();
			DPFTLogger.info(this, "Executing cdFTP+ Command from bash...");
			execCdFTP();
		}
	}

	private int execCdFTP() throws DPFTRuntimeException {
		if(ftp_cmd_file_name == null)
			return GlobalConstants.ERROR_LEVEL_TRF_SUCCESS;
		
		DPFTBashRunner  br = new DPFTBashRunner();
		String repeat_number = DPFTEngine.getSystemProperties(profile + ".cpa");
		String retry_time = DPFTEngine.getSystemProperties(profile + ".cpw");
		String ser = DPFTEngine.getSystemProperties(profile + ".c");
		br.setBashCmd(String.format(cd_ftp_bash_cmd, repeat_number, retry_time, ser, ftp_cmd_file_name));
		br.setLogFile(ftp_logfile);
		try {
			int errorlevel = br.execute();
			if(errorlevel == GlobalConstants.ERROR_LEVEL_TRF_FAILURE){
				Object[] params = {ftp_cmd_file_name};
				throw new DPFTFileTransferException("SYSTEM", "DPFT0018E", params);
			}
			if(errorlevel == GlobalConstants.ERROR_LEVEL_READ_FAILURE){
				Object[] params = {ftp_cmd_file_name};
				throw new DPFTFileTransferException("SYSTEM", "DPFT0018E", params);
			}
			if(errorlevel == GlobalConstants.ERROR_LEVEL_TRF_SUCCESS){
				DPFTLogger.info(this, "FTP Success!!!");
			}
			ftp_cmd_file_name = null;
			return errorlevel;
		} catch (Exception e) {
			String fname = ftp_cmd_file_name;
			ftp_cmd_file_name = null;
			Object[] params = {fname};
			throw new DPFTFileTransferException("SYSTEM", "DPFT0018E", params, e);
		}
		
	}

	private void initFTP(String[] ctrl_list, String[] file_list) {
		h_file_list.clear();
		d_file_list.clear();
		if(ctrl_list != null){
			for(String cfile: ctrl_list){
				if(cfile == null)
					continue;
				h_file_list.add(cfile);
			}
		}
		
		for(String filename: file_list){
			if(filename == null)
				continue;
			
			if(isZFile(filename)){
				h_file_list.add(filename);
			}else{
				d_file_list.add(filename);
			}
		}
		timestamp = System.currentTimeMillis();
	}
	
	private boolean isZFile(String filename) {
		return filename.indexOf("Z") == 0;
	}

	private void write_D_File_OUT_FTPCmd2LocalDir() throws DPFTRuntimeException {
		if(d_file_list.isEmpty())
			return;
		
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if(!fdir.exists()){
			fdir.mkdirs();
		}
		
		DPFTPCmdBuilder cb = new DPFTPCmdBuilder(GlobalConstants.FTP_MODE_BINARY);
		cb.cd(getRemoteDir());
		cb.lcd(ldir);
		cb.mput(d_file_list.toArray(new String[d_file_list.size()]));
		cb.quit();
		
		if(cb.hasFTPCmd()){
			try{
				ftp_cmd_file_name = ldir + File.separator + "cmd" + File.separator + "dput_" + timestamp + ".FTP";
				ftp_logfile = ldir + File.separator + "log" + File.separator + "dput_" + timestamp;
				File outfile = new File(ftp_cmd_file_name);
				if(!outfile.getParentFile().exists())
					outfile.getParentFile().mkdirs();
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outfile), "UTF-8"));
				out.write(cb.toString());
				out.flush();
				out.close();
			}catch(Exception e){
				throw new DPFTFileTransferException("SYSTEM", "DPFT0017E", e);
			}
		}
	}
	
	private void write_H_File_OUT_FTPCmd2LocalDir() throws DPFTRuntimeException {
		if(h_file_list.isEmpty())
			return;
		
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if(!fdir.exists()){
			fdir.mkdirs();
		}
		
		DPFTPCmdBuilder cb = new DPFTPCmdBuilder(GlobalConstants.FTP_MODE_BINARY);
		cb.cd(getRemoteDir());
		cb.lcd(ldir);
		cb.mput(h_file_list.toArray(new String[h_file_list.size()]));
		cb.quit();
		
		if(cb.hasFTPCmd()){
			try{
				ftp_cmd_file_name = ldir + File.separator + "cmd" + File.separator + "hput_" + timestamp + ".FTP";
				ftp_logfile = ldir + File.separator + "log" + File.separator + "hput_" + timestamp;
				File outfile = new File(ftp_cmd_file_name);
				if(!outfile.getParentFile().exists())
					outfile.getParentFile().mkdirs();
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outfile), "UTF-8"));
				out.write(cb.toString());
				out.flush();
				out.close();
			}catch(Exception e){
				throw new DPFTFileTransferException("SYSTEM", "DPFT0019E", e);
			}
		}
	}

	public void doFTP_Del(String[] del_clist, String[] del_flist) throws DPFTRuntimeException {
		synchronized(lock){
			DPFTLogger.info(this, "Initializing cdFTP+ Process...");
			initFTP(del_clist, del_flist);
			DPFTLogger.info(this, "Get File Success, removing files from remote cd server...");
			del_Remote_File_OUT_FTPCmd2LocalDir();
			DPFTLogger.info(this, "Executing cdFTP+ Command from bash...");
			execCdFTP();
		}
	}
	
	public void doFTP_Move(String[] file_list, String remoteDir) throws DPFTRuntimeException {
		synchronized(lock){
			DPFTLogger.info(this, "Initializing cdFTP Process...");
			initFTP(file_list, file_list);
			DPFTLogger.info(this, "Get File Success, move files to another remote dir from remote cd server...");
			mov_Remote_File_OUT_FTPCmd2RemoteDir(remoteDir);
			DPFTLogger.info(this, "Executing cdFTP+ Command from bash...");
			execCdFTP();
		}
	}
	
	private void mov_Remote_File_OUT_FTPCmd2RemoteDir(String remoteDir) throws DPFTRuntimeException {
		String ldir = getLocalDir();
		File fdir = new File(ldir);
		if (!fdir.exists()) {
			fdir.mkdirs();
		}
		
		DPFTPCmdBuilder cb = new DPFTPCmdBuilder(GlobalConstants.FTP_MODE_BINARY);
		cb.cd(getRemoteDir());
		cb.prompt(false);
		cb.move(d_file_list.toArray(new String[d_file_list.size()]), remoteDir);
		cb.quit();
		
		if (cb.hasFTPCmd()) {
			try {
				ftp_cmd_file_name = ldir + File.separator + "cmd" + File.separator + "mov_" + timestamp + ".FTP";
				ftp_logfile = ldir + File.separator + "log" + File.separator + "mov_" + timestamp;
				File outfile = new File(ftp_cmd_file_name);
				if(!outfile.getParentFile().exists()) {
					outfile.getParentFile().mkdirs();
				}
				Writer out = new BufferedWriter(new OutputStreamWriter(
						new FileOutputStream(outfile), "UTF-8"));
				out.write(cb.toString());
				out.flush();
				out.close();
			} catch(Exception e) {
				throw new DPFTFileTransferException("SYSTEM", "DPFT0019E", e);
			}
		}
	}


}

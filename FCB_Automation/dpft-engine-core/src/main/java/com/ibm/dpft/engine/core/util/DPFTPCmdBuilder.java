package com.ibm.dpft.engine.core.util;

import com.ibm.dpft.engine.core.common.GlobalConstants;

public class DPFTPCmdBuilder {
	private StringBuilder sb = null;
	private boolean has_ftp_cmd = false;

	public DPFTPCmdBuilder(int trf_mode) {
		sb = new StringBuilder();
		sb.append("open").append(GlobalConstants.FILE_EOL).append(GlobalConstants.FILE_EOL);
		if(trf_mode == GlobalConstants.FTP_MODE_BINARY)
			sb.append("binary").append(GlobalConstants.FILE_EOL);
	}

	public boolean hasFTPCmd() {
		return has_ftp_cmd;
	}

	public void mput(String[] flist) {
		
		for(String fn : flist){
			sb.append("put ").append(fn).append(GlobalConstants.FILE_EOL);
			if(!has_ftp_cmd)
				has_ftp_cmd = true;
		}
	}

	public void cd(String remoteDir) {
		sb.append("cd ").append(remoteDir).append(GlobalConstants.FILE_EOL);
		if(!has_ftp_cmd)
			has_ftp_cmd = true;
	}

	public void lcd(String localDir) {
		
		sb.append("lcd ").append(localDir).append(GlobalConstants.FILE_EOL);
		if(!has_ftp_cmd)
			has_ftp_cmd = true;
	}
	
	public String toString(){
		return sb.toString();
	}

	public void quit() {
		
		sb.append("quit").append(GlobalConstants.FILE_EOL);
		if(!has_ftp_cmd)
			has_ftp_cmd = true;
	}

	public void prompt(boolean flg) {
		sb.append("prompt ").append((flg)?"on":"off").append(GlobalConstants.FILE_EOL);
		if(!has_ftp_cmd)
			has_ftp_cmd = true;
	}

	public void mget(String[] flist) {
		for(String fn : flist){
			sb.append("mget ").append(fn).append(GlobalConstants.FILE_EOL);
			if(!has_ftp_cmd)
				has_ftp_cmd = true;
		}
	}

	public void delete(String[] flist) {
		
		for(String fn : flist){
			sb.append("delete ").append(fn).append(GlobalConstants.FILE_EOL);
			if(!has_ftp_cmd)
				has_ftp_cmd = true;
		}
	}
	
	public void move(String[] flist, String remoteDir) {
		for(String fn : flist) {
			sb.append("rename ").append(fn).append(" " + remoteDir + fn).append(GlobalConstants.FILE_EOL);
			if(!has_ftp_cmd) {
				has_ftp_cmd = true;
			}
		}
	}
}

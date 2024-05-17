package com.ibm.dpft.engine.core.util;

import java.util.HashMap;

import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;

abstract public class DPFTFileFormatter {
	private DPFTFileMetaData h_file_meta = null;
	private DPFTFileMetaData d_file_meta = null;
	private String h_file_string = null;
	private String d_file_string = null;
	private String h_file_name   = null;
	private String d_file_name   = null;
	private String c_file_name   = null;
	private String z_file_name   = null;
	private String z_file_string = null;
	private String file_encoding = null;
	
	DPFTFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		setControlFileMeta(h_meta);
		setDataFileMeta(d_meta);
	}

	public abstract void format(DPFTDboSet rs) throws DPFTRuntimeException;
	public abstract HashMap<String, String> getFormatFileList();
	public abstract HashMap<String, String> getFormatFileCharset();

	public DPFTFileMetaData getControlFileMeta() {
		return h_file_meta;
	}

	public void setControlFileMeta(DPFTFileMetaData h_file_meta) {
		this.h_file_meta = h_file_meta;		
	}

	public DPFTFileMetaData getDataFileMeta() {
		return d_file_meta;
	}

	public void setDataFileMeta(DPFTFileMetaData d_file_meta) {
		this.d_file_meta = d_file_meta;
	}
	
	public boolean hasControlFile(){
		return h_file_meta.getFileName() != null;
	}
	
	public boolean hasZFile(){
		return d_file_meta.getZFilePattern() != null;
	}

	public String getControlFileString() {
		return h_file_string;
	}

	public void setControlFileString(String h_file_string) {
		this.h_file_string = h_file_string;
	}

	public String getDataFileString() {
		return d_file_string;
	}

	public void setDataFileString(String d_file_string) {
		this.d_file_string = d_file_string;
	}

	public String getControlFileName() {
		
		return h_file_name;
	}

	public String getDataFileName() {
		
		return d_file_name;
	}

	public void setControlFileName(String h_file_name) {
		this.h_file_name = h_file_name;
	}

	public void setDataFileName(String d_file_name) {
		this.d_file_name = d_file_name;
	}

	public String getFileEncoding() {
		return file_encoding;
	}

	public void setFileEncoding(String file_encoding) {
		this.file_encoding = file_encoding;
	}

	public boolean needDataCompression() {
		
		return d_file_meta.hasCompressFilePattern();
	}
	
	public void setCompressFileName(String name){
		c_file_name = name;
	}

	public String getCompressFileName() {
		
		return c_file_name;
	}

	public String[] getBatchCompressFiles() {
		String[] batch = null;
		if(this.hasControlFile()){
			batch = new String[2];
			batch[0] = d_file_name;
			batch[1] = h_file_name;
		}else{
			batch = new String[1];
			batch[0] = d_file_name;
		}
		return batch;
	}

	public HashMap<String, String[]> retrieveDataCompressionInfo() {
		HashMap<String, String[]> info = new HashMap<String, String[]>();
		info.put(getCompressFileName(), getBatchCompressFiles());
		return info;
	}

	public String[] getFiles() {
		String[] batch = new String[1];
		batch[0] = d_file_name;
		//batch[1] = h_file_name;
		return batch;
	}

	public String getZFileName() {
		return z_file_name;
	}

	public void setZFileName(String z_file_name) {
		this.z_file_name = z_file_name;
	}

	public String getZFileString() {
		return z_file_string;
	}

	public void setZFileString(String z_file_string) {
		this.z_file_string = z_file_string;
	}

	public String[] getControlFiles() {
		String[] batch = null;
		if(this.hasControlFile() && !this.needDataCompression()){
			batch = new String[1];
			batch[0] = h_file_name;
		}
		return batch;
	}

}

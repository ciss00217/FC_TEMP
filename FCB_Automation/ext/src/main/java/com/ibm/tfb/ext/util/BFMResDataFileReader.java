package com.ibm.tfb.ext.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTFileReadException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class BFMResDataFileReader extends DPFTFileReader {
	private String fdir = null;
	private int current_index = -1;

	public BFMResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
		super(dir, resFileDataLayoutDbo, chal_name);
		fdir  = dir;
	}
	

	@Override
	public boolean read(String filename, boolean move2Archive) throws DPFTRuntimeException {
		// if(exist(filename) && canRead() && layout != null){
		if (exist(filename) && layout != null) {
			fname = filename;
			ResFileDataLayoutDetailDboSet layout_detail = layout.getLayoutDetail();
			if (layout_detail == null) {
				current_index = -1;
				Object[] params = { layout.getString("data_layout_id") };
				throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0031E", params);
			}
			try {
				BFMBytesBuffer buffer = new BFMBytesBuffer(new File(fdir + File.separator + filename),
						layout.getEncoding());
				buffer.wrap();
				int line_no = 1;
				read_data.clear();

				for (int i = 0; i < buffer.getLineCount(); i++) {
					if (line_no == 1 && layout.getString("contain_header").equalsIgnoreCase("y")) {
						headerString = buffer.getLineAsString(line_no);
						line_no++;
						continue;
					}
					// parse line string
					// DPFTLogger.debug(this,
					// "Read Line No." + line_no + " from " + filename + " =>" +
					// buffer.getLineAsString(line_no));
					try {
						HashMap<String, String> row_data = (layout.isStaticMode())
								? readlineWithStaticLength(buffer.getLine(line_no), layout_detail)
								: readline(buffer.getLineAsString(line_no), layout_detail);
						if (isValidData(row_data)) {
							read_data.add(row_data);
						} else {
							Object[] params = { filename, String.valueOf(line_no) };
							// DPFTUtil.pushNotification(new
							// DPFTMessage("SYSTEM", "DPFT0050E", params));
						}
					} catch (Exception e) {
						DPFTLogger.error(this, "e:" + e);
						Object[] params = { filename, String.valueOf(line_no) };
						// DPFTUtil.pushNotification(new DPFTMessage("SYSTEM",
						// "DPFT0050E", params, e));
					}
					line_no++;
				}
				current_index = 0;
				return true;
			} catch (IOException e) {
				current_index = -1;
				Object[] params = { filename };
				throw new DPFTFileReadException("SYSTEM", "DPFT0032E", params, e);
			} finally {
				try {
					if (move2Archive)
						moveFile2CompleteFolder(filename);
				} catch (IOException ex) {
					current_index = -1;
					Object[] params = { filename };
					throw new DPFTFileReadException("SYSTEM", "DPFT0032E", params, ex);
				}
			}
		}
		current_index = -1;
		return false;
	}

	private HashMap<String, String> readlineWithStaticLength(byte[] line_data,
			ResFileDataLayoutDetailDboSet layout_detail)
			throws NumberFormatException, DPFTRuntimeException, UnsupportedEncodingException {
		String[] cols = layout_detail.getColumnsInOrder();
		HashMap<String, Integer> byte_len_map = layout_detail.getColumnsLengthMapping();
		HashMap<String, String> rtnData = new HashMap<String, String>();
		int index = 0;
		for (String col : cols) {
			int sub_len = byte_len_map.get(col);
			byte[] subline_data = new byte[sub_len];
			for (int i = index, j = 0; j < sub_len; i++, j++) {
				subline_data[j] = line_data[i];
				if (j + 1 == sub_len)
					index = i + 1;
			}

			String value = normalizedColValue(new String(subline_data, layout.getEncoding()),
					layout_detail.isNumber(col));
			if (layout_detail.isDate(col))
				value = layout_detail.normalizedDateString(col, value);

			DPFTLogger.debug(this, "Read RTN Col:" + col + " value:" + value);
			rtnData.put(col, value);
		}
		return rtnData;
	}

	private String normalizedColValue(String value, boolean isNumber) {
		if (value.equals("\"\""))
			return "";

		Pattern p = Pattern.compile("^\"|\"$");
		Matcher matcher = p.matcher(value);
		if (matcher.lookingAt()) {
			// find String pattern like "String"
			value = value.substring(1, value.length() - 1);
		}

		if (isNumber) {
			Pattern p2 = Pattern.compile("^0+(?!$)");
			Matcher matcher2 = p2.matcher(value);
			if (matcher2.lookingAt()) {
				// find String pattern like "String"
				value = value.replaceFirst("^0+(?!$)", "");
			}
		}
		return _trim(value);
	}

	private String _trim(String value) {
		Pattern p = Pattern.compile("[|]$");
		Matcher matcher = p.matcher(value);
		if (matcher.find())
			value = matcher.replaceAll("");
		return value.trim();
	}

	private boolean isValidData(HashMap<String, String> row_data) throws NumberFormatException, DPFTRuntimeException {
		int data_count = layout.getLayoutDetail().count();
		return data_count == row_data.size();
	}

	private HashMap<String, String> readline(String line, ResFileDataLayoutDetailDboSet layout_detail)
			throws NumberFormatException, DPFTRuntimeException {
		String[] col_values = line.split(layout.getDelimeter(), -1);
		String[] cols = layout_detail.getColumnsInOrder();
		HashMap<String, String> rtnData = new HashMap<String, String>();
		int i = 0;
		for (String col : cols) {
			String value = normalizedColValue(col_values[i], layout_detail.isNumber(col));
			if (layout_detail.isDate(col))
				value = layout_detail.normalizedDateString(col, value);

			rtnData.put(col, value);
			i++;
		}
		return rtnData;
	}

	private void moveFile2CompleteFolder(String filename) throws IOException {
		File cp_dir = new File(fdir + File.separator + "archive");
		if (!cp_dir.exists())
			cp_dir.mkdirs();

		File org_file = new File(fdir + File.separator + filename);
		File cp_file = new File(fdir + File.separator + "archive" + File.separator + filename);
		InputStream fin = new FileInputStream(org_file);
		OutputStream fout = new FileOutputStream(cp_file);
		byte[] buffer = new byte[1024];

		int length;
		// copy the file content in bytes
		while ((length = fin.read(buffer)) > 0) {
			fout.write(buffer, 0, length);
		}
		fin.close();
		fout.close();
		fin = null;
		fout = null;
		// delete the original file
		if (!org_file.delete()) {
			DPFTLogger.debug(this, "Cannot Delete File " + org_file.getAbsolutePath());
		} else {
			DPFTLogger.debug(this, "Delete File " + org_file.getAbsolutePath());
		}
	}
}
package com.ibm.dpft.engine.core.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTDataFormatException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.util.EncryptUtil;
import org.apache.commons.lang3.StringUtils;

public class DPFTCSVFileFormatter extends DPFTFileFormatter {
	private String delimeter = GlobalConstants.FILE_DELIMETER_COMMA;
	protected int num_output = 0;

	public DPFTCSVFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta) {
		super(h_meta, d_meta);
		delimeter = d_meta.getDelimeter();
	}

	@Override
	public void format(DPFTDboSet rs) throws DPFTRuntimeException {
		// if(rs.count() > 0){
		formatDataString(rs);
		if (this.hasControlFile())
			formatControlFileString(rs);
		if (this.hasZFile())
			formatZFileString(rs);
		// }
	}

	protected void formatZFileString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		DPFTFileMetaData meta = this.getDataFileMeta();
		if (meta.hasCompressFilePattern()) {
			String filename = this.getCompressFileName() + ".zip";
			String header = meta.getZFilePattern().substring(1, 7);
			sb.append(String.format("%1$-10s", header)).append(String.format("%1$-40s", filename))
					.append(String.format("%1$-10s", "PUT")).append(String.format("%1$-40s", filename));
		}

		this.setZFileString(sb.toString());

		/* build z file name */
		this.setZFileName(buildFileNameByPattern(meta.getZFilePattern(), rs.getDbo(0)));
	}

	protected void formatControlFileString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		DPFTFileMetaData meta = this.getControlFileMeta();
		if (getDataFileMeta().isStaticColLength()) {
			if (meta.getActionID() != null)
				sb.append(meta.getActionID()).append(String.format("%1$010d", num_output));
			else
				sb.append(String.format("%1$010d", num_output));
		} else {
			/* build data header String */
			if (meta.isContainHeader()) {
				if (meta.getActionID() != null)
					sb.append(buildHeaderString(GlobalConstants.H_HEADER)).append(GlobalConstants.FILE_EOL);
				else
					sb.append(buildHeaderString(GlobalConstants.H_HEADER2)).append(GlobalConstants.FILE_EOL);
			}
			if (meta.getActionID() != null)
				sb.append(meta.getActionID()).append(delimeter).append(num_output);
			else
				sb.append(num_output);
		}
		this.setControlFileString(sb.toString());

		/* build control file name */
		this.setControlFileName(buildFileNameByPattern(meta.getFileName(), rs.getDbo(0)));
	}

	protected String buildHeaderString(String[] cols) {

		StringBuilder sb = new StringBuilder();
		for (String col : cols) {
			sb.append(col).append(delimeter);
		}
		DPFTLogger.debug(this, "buildHeaderString : " + sb.toString());
		return sb.substring(0, sb.length() - 1);
	}

	protected void formatDataString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		DPFTFileMetaData meta = this.getDataFileMeta();
		/* build data header String */
		if (meta.isContainHeader()) {
			sb.append(buildHeaderString(meta.getFileColsInOrder())).append(GlobalConstants.FILE_EOL);
		}

		/* build data body */
		num_output = 0;
		for (int i = 0; i < rs.count(); i++) {
			if (!canOutputRecord(rs.getDbo(i)))
				continue;
			sb.append(buildDataString(rs.getDbo(i), meta)).append(GlobalConstants.FILE_EOL);
			num_output++;
		}

		/* if file transfer validation active, append validation String */
		if (meta.needTransferValidation()) {
			String FILE_TRF_CNST = GlobalConstants.FILE_TRF_CNST;
			if (!StringUtils.isEmpty(meta.getTrf_cnst())) {
				FILE_TRF_CNST = meta.getTrf_cnst();
			}
			if (meta.getStaticTrf_cnst() > 0) {
				sb.append(FILE_TRF_CNST).append(
						String.format("%1$0" + meta.getStaticTrf_cnst() + "d", num_output));
			} else {
				sb.append(FILE_TRF_CNST).append(num_output);
			}
		}
		if (num_output != 0) {
			this.setDataFileString(sb.toString());
		}
		/* build data file name */
		this.setDataFileName(buildFileNameByPattern(meta.getFileName(), rs.getDbo(0)));

		/* build compression file name if needed */
		if (meta.hasCompressFilePattern()) {
			this.setCompressFileName(buildFileNameByPattern(meta.getCompressFilePattern(), rs.getDbo(0)));

		}
	}

	protected boolean canOutputRecord(DPFTDbo dbo) {
		return dbo.getString("process_status").equals(GlobalConstants.O_DATA_OUTPUT);
	}

	protected String buildFileNameByPattern(String pattern, DPFTDbo dbo) throws DPFTRuntimeException {
		if (pattern == null)
			return null;

		StringBuilder sb = new StringBuilder();
		StringBuilder sb_tmp = new StringBuilder();
		boolean is_pattern = false;
		for (char c : pattern.toCharArray()) {
			if (c == '{') {
				is_pattern = true;
				sb_tmp = new StringBuilder();
				continue;
			}
			if (c == '}') {
				is_pattern = false;
				try {
					sb.append(getStringByPatternKeyword(sb_tmp.toString(), dbo));
				} catch (DPFTInvalidSystemSettingException e) {
					Object[] params = { pattern };
					throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0022E", params, e);
				}
				continue;
			}
			if (is_pattern) {
				sb_tmp.append(c);
			} else {
				sb.append(c);
			}
		}
		DPFTLogger.info(this, "Build File Name : " + sb.toString());
		return sb.toString();
	}

	private String getStringByPatternKeyword(String pstr, DPFTDbo dbo) throws DPFTRuntimeException {
		if (pstr.indexOf("$") != -1) {
			/* System Define Constants */
			if (pstr.indexOf(GlobalConstants.FILE_PATTERN_CNST_SYSDATE) != -1) {
				String dataformat = (pstr.indexOf(GlobalConstants.FILE_PATTERN_CNST_SYSDATETIME) != -1)
						? GlobalConstants.DFPT_DATETIME_FORMAT : GlobalConstants.DFPT_DATE_FORMAT;

				// $SYSDATE, $SYSDATETIME
				if (pstr.indexOf(":") != -1) {
					/* Support T-1, T-2 as operator for current system time */
					String[] strs = pstr.split(":");
					if (strs.length == 3)
						dataformat = strs[2];
					SimpleDateFormat sdf = new SimpleDateFormat(dataformat);
					Calendar cal = Calendar.getInstance();
					cal.add(Calendar.DAY_OF_MONTH, getDayDiff(strs[1]));
					return sdf.format(cal.getTime());
				}
				SimpleDateFormat sdf = new SimpleDateFormat(dataformat);
				return sdf.format(new Date());
			} else if (pstr.indexOf(GlobalConstants.FILE_PATTERN_CNST_SYSPROP) != -1) {
				// $SYSPROP
				String[] strs = pstr.split(":");
				if (strs.length != 2) {
					Object[] params = { pstr };
					throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0022E", params);
				}
				return DPFTEngine.getSystemProperties(strs[1]);
			} else if (pstr.indexOf(GlobalConstants.FILE_PATTERN_CNST_YYMMDD) != -1) {
				// $YYMMDD
				String dataformat = GlobalConstants.DFPT_YYMMDD_FORMAT;
				SimpleDateFormat sdf = new SimpleDateFormat(dataformat);
				return sdf.format(new Date());
			} else if (pstr.indexOf(GlobalConstants.FILE_PATTERN_CNST_HHMMSS) != -1) {
				// $YYMMDD
				String dataformat = GlobalConstants.DFPT_HHMMSS_FORMAT;
				SimpleDateFormat sdf = new SimpleDateFormat(dataformat);
				return sdf.format(new Date());
			}

		}
		return dbo.getString(pstr);
	}

	private int getDayDiff(String str) throws DPFTRuntimeException {
		if (str == null || str.isEmpty())
			return 0;
		if (str.equals("T"))
			return 0;
		if (str.length() < 2)
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0021E");
		return Integer.valueOf(str.substring(1));
	}

	protected String buildDataString(DPFTDbo dbo, DPFTFileMetaData meta, boolean decrypt, int col_order)
			throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		if (meta.isStaticColLength()) {
			for (String col : meta.getColsOrder()) {
				if (meta.isDatetime(col)) {
					String val = getStringWithStaticColLength(getDateString(dbo.getDate(col), meta.getDateFormat(col)),
							meta.getColumnStaticLength(col), meta.isColumnNumeric(col), meta.isFullWidth(col),
							meta.getStaticLengUnit());
					sb.append(val);
				} else {
					String val = getStringWithStaticColLength(dbo.getColumnValue(col), meta.getColumnStaticLength(col),
							meta.isColumnNumeric(col), meta.isFullWidth(col), meta.getStaticLengUnit());
					sb.append(val);
				}
			}
		} else {
			for (int i = 0; i < meta.getColsOrder().length; i++) {
				String col = meta.getColsOrder()[i];
				if (meta.isDatetime(col)) {
					sb.append(getDateString(dbo.getDate(col), meta.getDateFormat(col))).append(delimeter);
				} else if (("RS_COUNT").equals(col)) {
					sb.append(num_output);
				} else {
					if (i == col_order && decrypt) {
						sb.append(EncryptUtil.IdDecrypt(getStringWithoutDelimeter(dbo.getColumnValue(col))))
								.append(delimeter);
					} else {
						sb.append(getStringWithoutDelimeter(dbo.getColumnValue(col))).append(delimeter);
					}
				}

			}
		}
		if (meta.isStaticColLength())
			return sb.toString();
		// DPFTLogger.debug(this, "buildDataString : " + sb.toString());
		return sb.substring(0, sb.length() - 1);
	}

	protected String buildDataString(DPFTDbo dbo, DPFTFileMetaData meta) throws DPFTRuntimeException {
		return buildDataString(dbo, meta, false, 0);
	}

	// Changed to protected for FCBLin to override
	protected String getDateString(Date date, String dateFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}

	private String getStringWithStaticColLength(Object columnValue, int length, boolean isColNumeric,
			boolean isFullWidth, String static_len_unit) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		if (columnValue == null) {
			String format = "%1$-" + length + "s";
			sb.append(String.format(format, " "));
			return sb.toString();
		}

		if (columnValue instanceof String) {
			String s = (String) columnValue;
			if (isColNumeric) {
				String format = "%1$0" + length;
				if (s.indexOf(".") != -1) {
					// float value
					int precision = s.length() - s.indexOf(".") - 1;
					format = format + "." + precision + "f";
					sb.append(String.format(format, Float.valueOf(s)));
				} else {
					// non-float value
					format = format + "d";
					sb.append(String.format(format, Long.valueOf(s)));
				}
			} else {
				s = (isFullWidth) ? DPFTUtil.convertToFullWidth(s) : s;
				String format = null;
				if (static_len_unit.equals(GlobalConstants.FILE_STATIC_LEN_UNIT_CHAR)) {
					s = truncateCharLength(s, length);
					format = "%1$-" + length + "s";
				} else {
					int adj_len = length;
					if (this.getFileEncoding().equals(GlobalConstants.FILE_ENCODE_UTF8)) {
						s = truncateUTF8(s, length);
						adj_len = getAdjustedLengthUTF8(s, length);
					} else if (this.getFileEncoding().equals(GlobalConstants.FILE_ENCODE_BIG5)) {
						s = truncateBig5(s, length);
						adj_len = getAdjustedLengthBig5(s, length);
					}

					format = "%1$-" + adj_len + "s";
				}
				DPFTLogger.debug(this, "Format=" + format + " String:" + s);
				sb.append(String.format(format, s));
			}
		}
		return sb.toString();
	}

	private String truncateCharLength(String s, int length) {
		if (s.length() > length) {
			return s.substring(0, length);
		}
		return s;
	}

	private String truncateBig5(String s, int n) throws DPFTRuntimeException {
		byte[] big5;
		try {
			big5 = s.getBytes(GlobalConstants.FILE_ENCODE_BIG5);
		} catch (UnsupportedEncodingException e) {
			throw new DPFTDataFormatException("SYSTEM", "DPFT0013E", e);
		}
		if (big5.length < n)
			n = big5.length;
		int n16 = 0;
		int i = 0;
		byte[] array = new byte[2];
		while (i < n) {
			int adv = 1;
			if (i + 1 < big5.length) {
				array[0] = big5[i];
				array[1] = big5[i + 1];

				// 兩個位元組轉換為整數
				int tmp = (short) ((array[0] << 8) | (array[1] & 0xff));
				tmp = tmp & 0xFFFF;

				// 判斷是否為BIG5漢字
				if (tmp >= 0xA440 && tmp < 0xFFFF) {
					if (i + 1 >= n)
						// over boundary, ignore this 2-byte character
						break;
					adv = 2;
				}

				// 判斷是否為BIG5特殊字元、標點符號
				if (tmp >= 0xA140 && tmp < 0xA3BF) {
					if (i + 1 >= n)
						// over boundary, ignore this 2-byte character
						break;
					adv = 2;
				}
			}
			i += adv;
			n16++;
		}
		return s.substring(0, n16);
	}

	private String truncateUTF8(String s, int n) throws DPFTRuntimeException {
		byte[] utf8;
		try {
			utf8 = s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new DPFTDataFormatException("SYSTEM", "DPFT0013E", e);
		}
		if (utf8.length < n)
			n = utf8.length;
		int n16 = 0;
		int advance = 1;
		int i = 0;
		while (i < n) {
			advance = 1;
			if ((utf8[i] & 0x80) == 0)
				i += 1;
			else if ((utf8[i] & 0xE0) == 0xC0)
				i += 2;
			else if ((utf8[i] & 0xF0) == 0xE0)
				i += 3;
			else {
				i += 4;
				advance = 2;
			}
			if (i <= n)
				n16 += advance;
		}
		return s.substring(0, n16);
	}

	private int getAdjustedLengthBig5(String s, int length) throws DPFTRuntimeException {
		byte[] big5;
		try {
			big5 = s.getBytes(GlobalConstants.FILE_ENCODE_BIG5);
		} catch (UnsupportedEncodingException e) {
			throw new DPFTDataFormatException("SYSTEM", "DPFT0013E", e);
		}

		if (s.length() == big5.length) {
			// No double-bytes characters
			return length;
		}

		int number_of_2_bytes_characters = big5.length - s.length();
		int adj_len = length - number_of_2_bytes_characters;
		return adj_len;
	}

	private int getAdjustedLengthUTF8(String s, int length) throws DPFTRuntimeException {
		byte[] utf8;
		try {
			utf8 = s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new DPFTDataFormatException("SYSTEM", "DPFT0013E", e);
		}

		if (s.length() == utf8.length) {
			// No double-bytes characters
			return length;
		}

		int i = 0;
		int number_of_muti_bytes_characters = 0;
		while (i < utf8.length) {
			// String contain double-bytes characters, such as Chinese..
			if ((utf8[i] & 0x80) == 0)
				i += 1;
			else if ((utf8[i] & 0xE0) == 0xC0) {
				i += 2;
				number_of_muti_bytes_characters += 1;
			} else if ((utf8[i] & 0xF0) == 0xE0) {
				i += 3;
				number_of_muti_bytes_characters += 2;
			} else {
				i += 4;
				number_of_muti_bytes_characters += 3;
			}
		}
		int adj_len = length - number_of_muti_bytes_characters;
		return adj_len;
	}

	// Changed to protected for APP Channel
	protected String getStringWithoutDelimeter(Object columnValue) {
		if (columnValue == null)
			return "";
		if (columnValue instanceof String) {
			String s = (String) columnValue;
			if (delimeter.equals(GlobalConstants.FILE_DELIMETER_COMMA))
				return s.replaceAll(GlobalConstants.FILE_DELIMETER_COMMA, GlobalConstants.FILE_DELIMETER_COMMA_FULL);
			else if (delimeter.equals(GlobalConstants.FILE_DELIMETER_SEMICOLON))
				return s.replaceAll(GlobalConstants.FILE_DELIMETER_SEMICOLON,
						GlobalConstants.FILE_DELIMETER_SEMICOLON_FULL);
			return s;
		}
		return null;
	}

	public String getDelimeter() {
		return delimeter;
	}

	public void setDelimeter(String delimeter) {
		this.delimeter = delimeter;
	}

	protected int getOutputQty() {
		return num_output;
	}

	protected void setOutputQty(int num_output) {
		this.num_output = num_output;
	}

	@Override
	public HashMap<String, String> getFormatFileList() {
		HashMap<String, String> file_out_list = new HashMap<String, String>();
		if (!StringUtils.isEmpty(getDataFileString())) {
			if (hasControlFile()) {
				file_out_list.put(getControlFileName(), getControlFileString());
				file_out_list.put(getDataFileName(), getDataFileString());
			} else {
				file_out_list.put(getDataFileName(), getDataFileString());
			}

			/* has z file for encryption */
			if (hasZFile()) {
				file_out_list.put(getZFileName(), getZFileString());
			}
		}
		return file_out_list;
	}

	@Override
	public HashMap<String, String> getFormatFileCharset() {
		HashMap<String, String> file_charset_list = new HashMap<String, String>();
		if (hasControlFile()) {
			file_charset_list.put(getControlFileName(), getFileEncoding());
			file_charset_list.put(getDataFileName(), getFileEncoding());
		} else {
			file_charset_list.put(getDataFileName(), getFileEncoding());
		}

		/* has z file for encryption */
		if (hasZFile()) {
			file_charset_list.put(getZFileName(), "UTF-8");
		}
		return file_charset_list;
	}

}

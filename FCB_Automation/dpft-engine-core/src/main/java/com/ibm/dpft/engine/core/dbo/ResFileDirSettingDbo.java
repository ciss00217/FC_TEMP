package com.ibm.dpft.engine.core.dbo;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.exception.DPFTFileReadException;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class ResFileDirSettingDbo extends DPFTDbo {
	private String[] flist = null;
	private String[] clist = null;
	private DPFTFileReader d_file_reader = null;
	private DPFTFileReader h_file_reader = null;

	public ResFileDirSettingDbo(String dboname, HashMap<String, Object> data, DPFTDboSet thisDboSet) {
		super(dboname, data, thisDboSet);

	}

	public String[] getRemoteFileName() throws DPFTRuntimeException {
		if (flist == null) {
			flist = new String[1];
			flist[0] = buildFileNameFromPattern(this.getString("d_file"));
			initFileReader();
		}
		return flist;
	}

	public String[] getRemoteCtrlName() throws DPFTRuntimeException {
		if (clist == null) {
			clist = new String[1];
			if (buildFileNameFromPattern(this.getString("h_file")) != null) {
				clist[0] = buildFileNameFromPattern(this.getString("h_file"));
			} else {
				clist = null;
			}
			initFileReader();
		}
		return clist;
	}

	public void initFileReader() throws DPFTRuntimeException {

		if (h_file_reader == null) {
			h_file_reader = getLocalControlFileReader(this.getString("chal_name"));
		}
		if (d_file_reader == null)
			d_file_reader = getLocalFileReader(this.getString("chal_name"));
	}

	public DPFTFileReader getLocalControlFileReader() throws DPFTRuntimeException {
		initFileReader();
		return h_file_reader;
	}

	public DPFTFileReader getLocalFileReader() throws DPFTRuntimeException {
		initFileReader();
		return d_file_reader;
	}

	protected DPFTFileReader getLocalFileReader(String chal_name) throws DPFTRuntimeException {
		if (d_file_reader != null)
			return d_file_reader;

		try {
			return getDataFileReader(chal_name);
		} catch (Exception e) {
			DPFTLogger.error(this, "getDataFileReader(chal_name) error:"+e.toString());
			throw new DPFTFileReadException("SYSTEM", "DPFT0023E");
		}
	}

	protected DPFTFileReader getDataFileReader(String chal_name)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException, ClassNotFoundException, DPFTRuntimeException, Exception {
		if (this.isNull("reader_class"))
			return new DPFTFileReader(this.getString("ldir"), getFileLayout(), chal_name);
		return (DPFTFileReader) Class.forName(this.getString("reader_class"))
				.getConstructor(String.class, ResFileDataLayoutDbo.class, String.class)
				.newInstance(this.getString("ldir"), getFileLayout(), chal_name);
	}

	public DPFTFileReader getLocalControlFileReader(String chal_name) throws DPFTRuntimeException {
		if (this.isNull("h_file"))
			return null;

		if (h_file_reader != null)
			return h_file_reader;
		return new DPFTFileReader(this.getString("ldir"), getControlFileLayout(), chal_name);
	}

	private ResFileDataLayoutDbo getControlFileLayout() throws DPFTRuntimeException {
		ResFileDataLayoutDboSet set = (ResFileDataLayoutDboSet) this.getDboSet("DPFT_RES_DATA_LAYOUT",
				"data_layout_id='" + this.getString("ctrl_layout_id") + "' and active=1");
		if (set.isEmpty()) {
			set.close();
			Object[] params = { this.getString("ctrl_layout_id") };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0029E", params);
		}
		ResFileDataLayoutDbo layout = (ResFileDataLayoutDbo) set.getDbo(0);
		set.close();
		return layout;
	}

	private ResFileDataLayoutDbo getFileLayout() throws DPFTRuntimeException {
		ResFileDataLayoutDboSet set = (ResFileDataLayoutDboSet) this.getDboSet("DPFT_RES_DATA_LAYOUT",
				"data_layout_id='" + this.getString("data_layout_id") + "' and active=1");
		if (set.isEmpty()) {
			set.close();
			Object[] params = { this.getString("data_layout_id") };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0030E", params);
		}
		ResFileDataLayoutDbo layout = (ResFileDataLayoutDbo) set.getDbo(0);
		set.close();
		return layout;
	}

	private String buildFileNameFromPattern(String pattern) throws DPFTRuntimeException {
		if (pattern == null || pattern.isEmpty())
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
					sb.append(getStringByPatternKeyword(sb_tmp.toString()));
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
		DPFTLogger.debug(this, "Remote RES File Name : " + sb.toString());
		return sb.toString();
	}

	private String getStringByPatternKeyword(String pstr) throws DPFTRuntimeException {
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
			}
		}
		return null;
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

	public String getControlFileName() throws DPFTRuntimeException {
		if (getRemoteCtrlName() != null)
			return getRemoteCtrlName()[0];
		else
			return null;
	}

	public String getDataFileName() throws DPFTRuntimeException {
		return getRemoteFileName()[0];
	}

	public boolean hasControlFile() {
		return !isNull("h_file");
	}

}

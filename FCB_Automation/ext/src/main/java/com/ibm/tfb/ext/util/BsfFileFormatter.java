package com.ibm.tfb.ext.util;

import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.FileDictionaryDboSet;
import com.ibm.dpft.engine.core.dbo.FileMetaDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.meta.DPFTFileMetaData;
import com.ibm.dpft.engine.core.util.DPFTCSVFileFormatter;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class BsfFileFormatter extends DPFTCSVFileFormatter {
	private String p_file_name = null;
	private String p_file_string = null;
	private String chal_name = null;
	private DPFTFileMetaData props_meta = null;
	private String i_file_name = null;
	private String i_file_string = null;
	private String add_chal = null;
	private DPFTFileMetaData info_meta = null;
	private int info_num_output = 0;

	public BsfFileFormatter(DPFTFileMetaData h_meta, DPFTFileMetaData d_meta, String chal_name, String add_chal)
			throws DPFTRuntimeException {
		super(h_meta, d_meta);
		this.chal_name = chal_name;
		props_meta = getPropsFileMeta();
		this.add_chal = add_chal;
		info_meta = getInfoFileMeta();
	}

	private DPFTFileMetaData getInfoFileMeta() throws DPFTRuntimeException {
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig());
		FileMetaDefDboSet meta = (FileMetaDefDboSet) connector.getDboSet("DPFT_FILE_META_DEF",
				"chal_name='" + add_chal + "' and active=1");
		FileDictionaryDboSet dicSet = (FileDictionaryDboSet) connector.getDboSet("DPFT_FILE_DIC",
				"chal_name='" + add_chal + "' and active=1");
		meta.load();
		dicSet.load();
		meta.close();
		dicSet.close();
		return new DPFTFileMetaData(meta, dicSet);
	}

	private DPFTFileMetaData getPropsFileMeta() throws DPFTRuntimeException {
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig());
		FileMetaDefDboSet meta = (FileMetaDefDboSet) connector.getDboSet("DPFT_FILE_META_DEF",
				"chal_name='" + chal_name + "' and active=1");
		FileDictionaryDboSet dicSet = (FileDictionaryDboSet) connector.getDboSet("DPFT_FILE_DIC",
				"chal_name='" + chal_name + "' and active=1");
		meta.load();
		dicSet.load();
		meta.close();
		dicSet.close();
		return new DPFTFileMetaData(meta, dicSet);
	}

	@Override
	public void format(DPFTDboSet rs) throws DPFTRuntimeException {
		formatDataString(rs);
		if (this.hasControlFile())
			formatControlFileString(rs);
		if (hasInfoFile()) {
			formatInfoFileString(rs);
		}
		if (hasPropsFile()) {
			formatPropsFileString(rs);
		}

	}

	@Override
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
			sb.append(buildDataString(rs.getDbo(i), meta, true, 4)).append(GlobalConstants.FILE_EOL);
			num_output++;
		}

		/* if file transfer validation active, append validation String */
		if (meta.needTransferValidation()) {
			String FILE_TRF_CNST = GlobalConstants.FILE_TRF_CNST;
			if (!StringUtils.isEmpty(meta.getTrf_cnst())) {
				FILE_TRF_CNST = meta.getTrf_cnst();
			}
			if (meta.getStaticTrf_cnst() > 0) {
				sb.append(FILE_TRF_CNST).append(String.format("%1$0" + meta.getStaticTrf_cnst() + "d", num_output));
			} else {
				sb.append(FILE_TRF_CNST).append(num_output);
			}
		}
		if (num_output != 0) {
			// DPFTLogger.debug(this, "DataFileString:" + sb.toString());
			this.setDataFileString(sb.toString());
		}
		/* build data file name */
		this.setDataFileName(buildFileNameByPattern(meta.getFileName(), rs.getDbo(0)));

		/* build compression file name if needed */
		if (meta.hasCompressFilePattern()) {
			this.setCompressFileName(buildFileNameByPattern(meta.getCompressFilePattern(), rs.getDbo(0)));

		}
	}

	private boolean hasInfoFile() {
		return info_meta.getFileName() != null;
	}

	private boolean hasPropsFile() {
		return props_meta.getFileName() != null;
	}

	protected void formatPropsFileString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		/* build data header String */
		if (props_meta.isContainHeader()) {
			sb.append(buildHeaderString(props_meta.getFileColsInOrder())).append(GlobalConstants.FILE_EOL);
		}

		/* build data body */
		StringBuilder body = new StringBuilder();
		body.append(buildDataString(rs.getDbo(0), props_meta));
		if (info_num_output > 0) {
			body.append(this.getInfoFileName()).append(getDelimeter()).append(this.info_num_output);
		}
		String bodyString = body.toString().replace("_SFA", "SFA");
		sb.append(bodyString);
		sb.append(GlobalConstants.FILE_EOL);

		this.setPropsFileString(sb.toString());

		/* build data file name */
		this.setPropsFileName(buildFileNameByPattern(props_meta.getFileName(), rs.getDbo(0)));
	}

	protected void formatInfoFileString(DPFTDboSet rs) throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		/* build data header String */
		if (info_meta.isContainHeader()) {
			sb.append(buildHeaderString(info_meta.getFileColsInOrder())).append(GlobalConstants.FILE_EOL);
		}
		/* build data body */
		info_num_output = 0;
		for (int a = 0; a < rs.count(); a++) {
			if (!canOutputRecord(rs.getDbo(a)))
				continue;
			for (int i = 1; i <= 10; i++) {

				if (!StringUtils.isEmpty((String) rs.getDbo(a).getColumnValue("RESV" + i + "NAME"))) {
					String[] colsOrder = info_meta.getColsOrder();
					String[] resetCols = colsOrder.clone();
					for (int j = 0; j < colsOrder.length; j++) {
						if (colsOrder[j].equals("ADD_INFO_DESC")) {
							colsOrder[j] = "RESV" + i + "NAME";
						}
						if (colsOrder[j].equals("ADD_INFO_DATA")) {
							colsOrder[j] = "RESV" + i;
						}
					}
					info_meta.setColsOrder(colsOrder);
					sb.append(buildDataString(rs.getDbo(a), info_meta, true, 5)).append(Integer.toString(i))
							.append(GlobalConstants.FILE_EOL);
					info_meta.setColsOrder(resetCols);
					info_num_output++;
				}
			}
		}

		this.setInfoFileString(sb.toString());

		/* build data file name */
		this.setInfoFileName(buildFileNameByPattern(info_meta.getFileName(), rs.getDbo(0)));
	}

	public void setInfoFileName(String i_file_name) {
		this.i_file_name = i_file_name;
	}

	public String getInfoFileName() {
		return i_file_name;
	}

	public void setInfoFileString(String i_file_string) {
		this.i_file_string = i_file_string;
	}

	public String getInfoFileString() {
		return i_file_string;
	}

	public void setPropsFileName(String p_file_name) {
		this.p_file_name = p_file_name;
	}

	public String getPropsFileName() {
		return p_file_name;
	}

	public void setPropsFileString(String p_file_string) {
		this.p_file_string = p_file_string;
	}

	public String getPropsFileString() {
		return p_file_string;
	}

	@Override
	public String[] getFiles() {
		String[] batch = new String[3];
		batch[0] = getDataFileName();
		batch[1] = p_file_name;
		batch[2] = i_file_name;
		return batch;
	}

	@Override
	public HashMap<String, String> getFormatFileList() {
		HashMap<String, String> flist = new HashMap<String, String>();
		if (!StringUtils.isEmpty(getDataFileString())) {
			flist.put(getDataFileName(), getDataFileString());
			if (hasInfoFile() && info_num_output > 0) {
				flist.put(getInfoFileName(), getInfoFileString());
			}
			if (hasPropsFile()) {
				flist.put(getPropsFileName(), getPropsFileString());
			}
		}
		return flist;
	}

	@Override
	public HashMap<String, String> getFormatFileCharset() {
		HashMap<String, String> flist = super.getFormatFileCharset();
		if (hasInfoFile()) {
			flist.put(getInfoFileName(), info_meta.getFileEncode());
		}
		if (hasInfoFile()) {
			flist.put(getPropsFileName(), props_meta.getFileEncode());
		}
		return flist;
	}

}

package com.ibm.dpft.engine.core.meta;

import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.FileDictionaryDboSet;
import com.ibm.dpft.engine.core.dbo.FileMetaDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;

public class DPFTFileMetaData {
	private boolean contain_header = true;
	private HashMap<String, Integer> column_length_define_map = null;
	private HashMap<String, String> o_f_col_dictionary = null;
	private HashMap<String, Boolean> column_full_width_map = null;
	private HashMap<String, String> column_date_format_map = null;
	private String[] cols_order = null;
	private String[] numeric_cols = null;
	private String filename = null;
	private boolean need_transfer_validation = false;
	private boolean is_static_col_length = false;
	private String static_len_unit = null;
	private String compress_file = null;
	private String z_file = null;
	private String h_file_act_id = null;
	private String delimeter = null;
	private String encode = null;
	private String trf_cnst = null;
	private int is_static_trf_cnst = 0;

	public DPFTFileMetaData(FileMetaDefDboSet meta) throws DPFTRuntimeException {
		this(meta, null);
	}

	public DPFTFileMetaData(FileMetaDefDboSet meta, FileDictionaryDboSet dicSet) throws DPFTRuntimeException {
		if (dicSet == null) {
			// Ctrl file meta
			setFileName(meta.getControlFilePattern());
			setContainHeader(meta.isContainHeader());
			setActionID(meta.getActionID());

		} else {
			// Data file meta
			setFileName(meta.getDataFilePattern());
			setFileEncode(meta.getFileEncoding());
			initFileDictionary(dicSet);
			setStaticMode(meta.isStaticMode());
			setContainHeader(meta.isContainHeader());
			setTransferValidation(meta.needTransferValidate());
			setTrf_cnst(meta.getTrf_cnst());
			setStaticTrf_cnst(meta.isStaticTrf_cnst());
			setStaticLengUnit(meta.getStaticLengUnit());
			if (meta.needCompress()) {
				setCompressFilePattern(meta.getCompressFilePattern());
			}
			if (meta.hasZFile()) {
				setZFilePattern(meta.getZFilePattern());
			}
			if (!meta.isStaticMode())
				setDelimeter(meta.getDelimeter());

		}
	}

	public HashMap<String, Integer> getColumnLengthDefinition() {
		return column_length_define_map;
	}

	public void setColumnLengthDefinition(HashMap<String, Integer> column_length_define_map) {
		this.column_length_define_map = column_length_define_map;
	}

	public boolean isContainHeader() {
		return contain_header;
	}

	public void setContainHeader(boolean contain_header) {
		this.contain_header = contain_header;
	}

	public String getFileName() {
		return filename;
	}

	public void setFileName(String filename) {
		this.filename = filename;
	}

	public void setOutputColumnsInOrders(String[] cols_order) {

		this.setColsOrder(cols_order);
	}

	public String[] getColsOrder() {
		return cols_order;
	}

	public void setColsOrder(String[] cols_order) {
		this.cols_order = cols_order;
	}

	public boolean needTransferValidation() {
		return need_transfer_validation;
	}

	public void setTransferValidation(boolean need_transfer_validation) {
		this.need_transfer_validation = need_transfer_validation;
	}

	public void setStaticMode(boolean flg) {
		is_static_col_length = flg;
	}

	public boolean isStaticColLength() {
		return is_static_col_length;
	}

	public String[] getNumericCols() {
		return numeric_cols;
	}

	public void setNumericCols(String[] numeric_cols) {
		this.numeric_cols = numeric_cols;
	}

	public boolean isColumnNumeric(String col) {

		for (String c : numeric_cols) {
			if (c.equalsIgnoreCase(col))
				return true;
		}
		return false;
	}

	public int getColumnStaticLength(String col) {

		return column_length_define_map.get(col);
	}

	public int getRowWidth() {

		int row_width = 0;
		for (String col : column_length_define_map.keySet()) {
			row_width += column_length_define_map.get(col);
		}
		return row_width;
	}

	public void initFileDictionary(FileDictionaryDboSet dicSet) throws DPFTRuntimeException {

		setColsOrder(dicSet.getColumnsOrder());
		setColumnLengthDefinition(dicSet.getColumnLengthSetting());
		setNumericCols(dicSet.getNumericColumns());
		setColDictionary(dicSet.getColumnNameDictionary());
		setColFullWidthDefinition(dicSet.setColFullWidthDefinition());
		setDateFormatDefinition(dicSet.getDateFormatDictionary());
	}

	private void setDateFormatDefinition(HashMap<String, String> map) {
		column_date_format_map = map;
	}

	private void setColFullWidthDefinition(HashMap<String, Boolean> map) {
		column_full_width_map = map;
	}

	public HashMap<String, String> getColDictionary() {
		return o_f_col_dictionary;
	}

	public void setColDictionary(HashMap<String, String> o_f_col_dictionary) {
		this.o_f_col_dictionary = o_f_col_dictionary;
	}

	public String[] getFileColsInOrder() {
		String[] rtnV = new String[cols_order.length];
		int i = 0;
		for (String col : cols_order) {
			rtnV[i] = o_f_col_dictionary.get(col);
			i++;
		}
		return rtnV;
	}

	public void setCompressFilePattern(String compressFilePattern) {

		compress_file = compressFilePattern;
	}

	public String getCompressFilePattern() {
		return compress_file;
	}

	public boolean hasCompressFilePattern() {

		return compress_file != null;
	}

	public String getZFilePattern() {
		return z_file;
	}

	public void setZFilePattern(String z_file) {
		this.z_file = z_file;
	}

	public String getActionID() {
		return h_file_act_id;
	}

	public void setActionID(String h_file_act_id) {
		this.h_file_act_id = h_file_act_id;
	}

	public boolean isFullWidth(String col) {
		return column_full_width_map.get(col);
	}

	public String getStaticLengUnit() {
		return static_len_unit;
	}

	public void setStaticLengUnit(String static_len_unit) {
		this.static_len_unit = static_len_unit;
	}

	public String getDelimeter() {
		if (delimeter == null)
			return GlobalConstants.FILE_DELIMETER_COMMA;
		if (delimeter.equals("AND"))
			return GlobalConstants.FILE_DELIMETER_AND;
		else if (delimeter.equals("SEMICOLON"))
			return GlobalConstants.FILE_DELIMETER_SEMICOLON;
		else if (delimeter.equals("TAB"))
			return GlobalConstants.FILE_DELIMETER_TAB;
		else if (delimeter.equals("SPIP"))
			return GlobalConstants.FILE_DELIMETER_SPIP;
		return GlobalConstants.FILE_DELIMETER_COMMA;
	}

	public void setDelimeter(String delimeter) {
		this.delimeter = delimeter;
	}

	public String getFileEncode() {
		return encode;
	}

	public void setFileEncode(String encode) {
		this.encode = encode;
	}

	public boolean isDatetime(String col) {
		return column_date_format_map.containsKey(col);
	}

	public String getDateFormat(String col) {
		return column_date_format_map.get(col);
	}

	public void setTrf_cnst(String trf_cnst) {
		this.trf_cnst = trf_cnst;
	}

	public String getTrf_cnst() {
		return this.trf_cnst;
	}

	public void setStaticTrf_cnst(int length) {
		is_static_trf_cnst = length;
	}

	public int getStaticTrf_cnst() {
		return is_static_trf_cnst;
	}
}

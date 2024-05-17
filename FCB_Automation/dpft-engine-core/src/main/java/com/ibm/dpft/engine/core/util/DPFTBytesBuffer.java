package com.ibm.dpft.engine.core.util;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

public class DPFTBytesBuffer {
	private byte[] all_bytes = null;
	private String encode = "utf8";
	private int line_count = 0;
	private HashMap<Integer, byte[]> line_bytes_map = new HashMap<Integer, byte[]>();
	private static final int BOM_SIZE = 4;

	public DPFTBytesBuffer(File read_file, String encode) throws IOException {
		if (read_file.exists()) {
			all_bytes = FileUtils.readFileToByteArray(read_file);
			this.encode = encode;
		}
	}

	public void wrap() {
		if (all_bytes == null)
			return;
		if (encode.toLowerCase().indexOf("utf") != -1)
			skipUnicodeBom();
		process2LineBytes();
	}

	private void skipUnicodeBom() {
		byte bom[] = new byte[BOM_SIZE];
		int bom_len = (all_bytes.length < bom.length) ? all_bytes.length : bom.length;
		int start_index = 0;
		System.arraycopy(all_bytes, 0, bom, 0, bom_len);
		if ((bom[0] == (byte) 0x00) && (bom[1] == (byte) 0x00) && (bom[2] == (byte) 0xFE) && (bom[3] == (byte) 0xFF)) {
			encode = "UTF-32BE";
			start_index = 4;
		} else if ((bom[0] == (byte) 0xFF) && (bom[1] == (byte) 0xFE) && (bom[2] == (byte) 0x00)
				&& (bom[3] == (byte) 0x00)) {
			encode = "UTF-32LE";
			start_index = 4;
		} else if ((bom[0] == (byte) 0xEF) && (bom[1] == (byte) 0xBB) && (bom[2] == (byte) 0xBF)) {
			encode = "UTF-8";
			start_index = 3;
		} else if ((bom[0] == (byte) 0xFE) && (bom[1] == (byte) 0xFF)) {
			encode = "UTF-16BE";
			start_index = 2;
		} else if ((bom[0] == (byte) 0xFF) && (bom[1] == (byte) 0xFE)) {
			encode = "UTF-16LE";
			start_index = 2;
		} else {
			// Unicode BOM mark not found, unread all bytes
			start_index = 0;
		}
		byte[] new_array = new byte[all_bytes.length - start_index];
		System.arraycopy(all_bytes, start_index, new_array, 0, new_array.length);
		all_bytes = new_array;
	}

	private void process2LineBytes() {
		ArrayList<Byte> bary = new ArrayList<Byte>();
		for (int i = 0; i < all_bytes.length; i++) {
			if (i + 1 < all_bytes.length) {
				if ((char) all_bytes[i] == '\r' && (char) all_bytes[i + 1] == '\n') {
					// CRLF reach
					line_count++;
					line_bytes_map.put(line_count, ArrayUtils.toPrimitive(bary.toArray(new Byte[bary.size()])));
					bary.clear();
					i = (i + 1);
					continue;
				} else if ((char) all_bytes[i] == '\n') {
					if (i - 1 >= 0) {
						if ((char) all_bytes[i - 1] == '\r') {
							// LF reach
							line_count++;
							line_bytes_map.put(line_count, ArrayUtils.toPrimitive(bary.toArray(new Byte[bary.size()])));
							bary.clear();
							i = (i + 1);
							continue;
						}
					}
				}
			}
			bary.add(all_bytes[i]);
		}
		if (!bary.isEmpty()) {
			line_count++;
			line_bytes_map.put(line_count, ArrayUtils.toPrimitive(bary.toArray(new Byte[bary.size()])));
			bary.clear();
		}
	}

	public int getLineCount() {
		return line_count;
	}

	public byte[] getLine(int i) {
		return line_bytes_map.get(i);
	}

	public String getLineAsString(int i) throws UnsupportedEncodingException {
		return new String(getLine(i), encode);
	}

}

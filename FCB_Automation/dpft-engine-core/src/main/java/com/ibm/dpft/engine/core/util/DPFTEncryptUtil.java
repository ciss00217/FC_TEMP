package com.ibm.dpft.engine.core.util;

import org.jasypt.util.text.BasicTextEncryptor;

public class DPFTEncryptUtil {
	private static final String decryptPass = "fcbabcdef0123456789abc";
	private static BasicTextEncryptor textEncryptor = null;

	private DPFTEncryptUtil() {
	}

	public String Decrypt(String encryptMessage) {

		String decrypt = textEncryptor.decrypt(encryptMessage);
		return decrypt;
	}

	public static BasicTextEncryptor getDecryptor() {
		if (textEncryptor == null) {
			synchronized (DPFTEncryptUtil.class) {
				if (textEncryptor == null) {
					textEncryptor = new BasicTextEncryptor();
					textEncryptor.setPassword(decryptPass);
				}
			}
		}
		return textEncryptor;
	}
}

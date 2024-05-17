package com.ibm.tfb.ext.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import com.google.gson.Gson;
import com.ibm.dpft.engine.core.DPFTEngine;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public class FCBDidSend {
	public String custId;
	public String op = "mgmApiForCM";
	public String ServiceURL;

	public FCBDidSend() {
		this.ServiceURL = DPFTEngine.getSystemProperties("DID.ServiceURL");
	}

	public String sendService(FCBDidSend data) throws Exception {
		FCBDidReceive recvVO = null;
		Gson gson = new Gson();
		try {
			String urlstr = ServiceURL;
			URL url = new URL(urlstr);
			StringBuilder sb = new StringBuilder();
			if ( urlstr.substring(0, 5).equals("https") ) {
				DPFTLogger.info(this, "API going https...");
				HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
				con.setRequestMethod("POST");
				con.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				DPFTLogger.info(this, "API request param: " + data.toSendParameters());
				wr.write(data.toSendParameters().getBytes("utf-8"));
				BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String line;
				while ((line = rd.readLine()) != null) {
					sb.append(line);
				}
			}
			else {
				DPFTLogger.info(this, "API going http...");
				HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("POST");
				con.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				DPFTLogger.info(this, "API request param: " + data.toSendParameters());
				wr.write(data.toSendParameters().getBytes("utf-8"));
				BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream()));
				String line;
				while ((line = rd.readLine()) != null) {
					sb.append(line);
				}
			}
			
			DPFTLogger.info(this, "API response: " + sb.toString());
			recvVO = gson.fromJson(sb.toString(), FCBDidReceive.class);
			DPFTLogger.info(this, "rt is : '" + recvVO.getResult().getRt() + "'");
			DPFTLogger.info(this, "workCode is : " + recvVO.getResult().getWorkCode());
			DPFTLogger.info(this, "did is : " + recvVO.getResult().getMsg().getPromotionCode());
			return recvVO.getResult().getMsg().getPromotionCode();
		} catch (Exception e) {
			throw (e);
		}
	}

	public String toSendParameters() {
		return "custId=" + this.custId + "&op=" + this.op;
	}
}

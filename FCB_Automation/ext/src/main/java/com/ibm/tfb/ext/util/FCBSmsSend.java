package com.ibm.tfb.ext.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import com.google.gson.Gson;
import com.ibm.dpft.engine.core.DPFTEngine;

public class FCBSmsSend {
	public String UID;
	public String Pwd;
	public String DA;
	public String SM;
	public String BUSINESSCODE;
	public String STOPTIME;
	public String AParty;
	public String TEMPLATEVAR;
	public String RowID;
	public String ServiceURL;

	public FCBSmsSend() {
		this.ServiceURL = DPFTEngine.getSystemProperties("SMS.ServiceURL");
	}

	public String sendService(FCBSmsSend data) throws Exception {
		FCBSmsReceive[] recvVO = null;
		Gson gson = new Gson();
		try {
			String urlstr = "http://" + ServiceURL + "/apifcb/Sms/SendSms"; // Test
			URL url = new URL(urlstr);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.write(data.toSendParameters().getBytes("utf-8"));
			BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String line;
			StringBuilder sb = new StringBuilder();
			while ((line = rd.readLine()) != null) {
				sb.append(line);
			}
			recvVO = gson.fromJson(sb.toString(), FCBSmsReceive[].class);
			if (recvVO[0].getRowId() != null) {
				return recvVO[0].getRowId();
			} else {
				return recvVO[0].getErrorCode();
			}
		} catch (Exception e) {
			throw (e);
		}
	}

	public String responseService(FCBSmsSend data) throws Exception {
		FCBSmsReceive[] recvVO = null;
		Gson gson = new Gson();
		try {
			String urlstr = "http://" + ServiceURL + "/apifcb/Sms/QuerySmsDr"; // Test
			URL url = new URL(urlstr);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(con.getOutputStream());
			wr.write(data.toResposneParameters().getBytes("UTF-8"));
			BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
			String line;
			StringBuilder sb = new StringBuilder();
			while ((line = rd.readLine()) != null) {
				sb.append(line);
			}
			recvVO = gson.fromJson(sb.toString(), FCBSmsReceive[].class);
			if (recvVO[0].getCheckMode() != null) {
				return recvVO[0].getCheckMode();
			} else {
				return recvVO[0].getErrorCode();
			}
		} catch (Exception e) {
			throw (e);
		}
	}

	public String toSendParameters() {
		return "UID=" + this.UID + "&PWD=" + this.Pwd + "&DA=" + this.DA + "&SM=" + this.SM + "&BUSINESSCODE="
				+ this.BUSINESSCODE + "&STOPTIME=" + this.STOPTIME + "&AParty=" + this.AParty + "&TEMPLATEVAR="
				+ this.TEMPLATEVAR;
	}

	public String toResposneParameters() {
		return "UID=" + this.UID + "&PWD=" + this.Pwd + "&RowId=" + this.RowID;
	}
}

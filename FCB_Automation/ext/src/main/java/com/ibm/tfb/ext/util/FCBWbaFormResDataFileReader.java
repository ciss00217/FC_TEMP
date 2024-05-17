package com.ibm.tfb.ext.util;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDbo;
import com.ibm.dpft.engine.core.dbo.ResFileDataLayoutDetailDboSet;
import com.ibm.dpft.engine.core.exception.DPFTInvalidSystemSettingException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTFileReader;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBConstants;
import com.ibm.tfb.ext.common.TFBUtil;

public class FCBWbaFormResDataFileReader extends DPFTFileReader {

	public FCBWbaFormResDataFileReader(String dir, ResFileDataLayoutDbo resFileDataLayoutDbo, String chal_name) {
		super(dir, resFileDataLayoutDbo, chal_name);
	}

	@Override
	public void write2TargetTable(String timestamp) throws DPFTRuntimeException {
		if (layout == null) {
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0033E");
		}

		ResFileDataLayoutDetailDboSet layout_detail = layout.getLayoutDetail();
		if (layout_detail == null) {
			Object[] params = { layout.getString("data_layout_id") };
			throw new DPFTInvalidSystemSettingException("SYSTEM", "DPFT0031E", params);
		}

		DPFTDboSet targetSet = layout.getTargetTableDboSet();
		HashMap<String, String> f_col_2_tgt_col_map = layout.getFileColumns2TargetColumnsMapping();
		
		DPFTConfig config = DPFTUtil.getSystemDBConfig();
		DPFTConnector connectorDID = DPFTConnectionFactory.initDPFTConnector(config);
		
		// Generate random seq for cust_id
		String[] seq_cust_list = TFBUtil.generateSEQ(TFBConstants.WBA_SEQ_CUST_ID, read_data.size());
		int j = 0;
		
		for (HashMap<String, String> rowdata : read_data) {
			DPFTDbo new_data = targetSet.add();
			new_data.setValue("process_time", timestamp);
			new_data.setValue("process_status", GlobalConstants.DPFT_OBND_STAT_STAGE);
			
			DPFTLogger.debug(this, "HAVE_DID column is: "+rowdata.get("have_did")+" and DID column is: "+rowdata.get("did"));
			// Map Referral Customer ID if exists
			if ( "Y".equals(rowdata.get("have_did")) && !"undefined".equals(rowdata.get("did"))) {
				StringBuilder sbpromotion = new StringBuilder();
				sbpromotion.append("PROMOTION_CODE = \'");
				sbpromotion.append(rowdata.get("did"));
				sbpromotion.append("\'");
				DPFTDboSet promotionCodeToIDSet = (DPFTDboSet) connectorDID.getDboSet(" CUST_ID ", "CMETL.CFMBSEL_STG", sbpromotion.toString());
				DPFTLogger.debug(this, "promotionCodeSet DBOSIZEHERE " + String.valueOf(promotionCodeToIDSet.count()));
				if ( promotionCodeToIDSet.count() > 0 ) {
					new_data.setValue("DID_ID", promotionCodeToIDSet.getDbo(0).getString("CUST_ID").trim());
				}
			}
			
			new_data.setValue("CUSTOMER_ID", seq_cust_list[j]);
			j+=1;
			System.out.println("!!!!!!!!!!!!!!!!!!f_col_2_tgt_col_map:" + f_col_2_tgt_col_map);
			System.out.println("!!!!!!!!!!!!!!!!!!rowdata:" + rowdata);
			
			for (String col : rowdata.keySet()) {
				if (f_col_2_tgt_col_map.get(col) == null)
					continue;
				new_data.setValue(f_col_2_tgt_col_map.get(col), rowdata.get(col));
			}

			// parse Json
			JSONObject obj = new JSONObject(rowdata.get("structure"));
			JSONArray ans_array = obj.getJSONArray("answer");

			int ans_len = ans_array.length();
			String[] questions = new String[ans_len]; // 依序存問題
			String[] answers = new String[ans_len]; // 依序存答案
			String[] replyType = new String[ans_len]; // 依序存答案

			new_data.setValue("FIELD26_NAME", "ID");
			new_data.setValue("FIELD27_NAME", "NAME");
			new_data.setValue("FIELD28_NAME", "TAIWAN_TELEPHONE");
			new_data.setValue("FIELD29_NAME", "EMAIL");
			new_data.setValue("FIELD30_NAME", "EXPIRYDATE");
			
			for (int i = 0; i < ans_len; i++) {
				parseJsonToArray(ans_array, questions, answers, replyType, i);
				switch (replyType[i]) {
				case "id":
					if (checkID(answers[i])) {
						new_data.setValue("FIELD26_NAME", questions[i]);
						new_data.setValue("FIELD26_VALUE", encryptID(answers[i],connectorDID));
						new_data.setValue("FIELD" + (i + 1) + "_NAME", questions[i]);
						new_data.setValue("FIELD" + (i + 1) + "_VALUE", answers[i]);
					}
					break;
				case "name":
					new_data.setValue("FIELD27_NAME", questions[i]);
					new_data.setValue("FIELD27_VALUE", answers[i]);
					new_data.setValue("FIELD" + (i + 1) + "_NAME", questions[i]);
					new_data.setValue("FIELD" + (i + 1) + "_VALUE", answers[i]);
					break;
				case "taiwan_telephone":
					new_data.setValue("FIELD28_NAME", questions[i]);
					new_data.setValue("FIELD28_VALUE", answers[i]);
					new_data.setValue("FIELD" + (i + 1) + "_NAME", questions[i]);
					new_data.setValue("FIELD" + (i + 1) + "_VALUE", answers[i]);
					break;
				case "email":
					new_data.setValue("FIELD29_NAME", questions[i]);
					new_data.setValue("FIELD29_VALUE", answers[i]);
					new_data.setValue("FIELD" + (i + 1) + "_NAME", questions[i]);
					new_data.setValue("FIELD" + (i + 1) + "_VALUE", answers[i]);
					break;
				case "expiryDate":
					new_data.setValue("FIELD30_NAME", questions[i]);
					new_data.setValue("FIELD30_VALUE", answers[i]);
					new_data.setValue("FIELD" + (i + 1) + "_NAME", questions[i]);
					new_data.setValue("FIELD" + (i + 1) + "_VALUE", answers[i]);
					break;
				default:
					new_data.setValue("FIELD" + (i + 1) + "_NAME", questions[i]);
					new_data.setValue("FIELD" + (i + 1) + "_VALUE", answers[i]);
				}
			}
		}
		
		targetSet.save();
		targetSet.close();
	}
	
	private boolean checkID(String _id) {
		Pattern entIdPattern = Pattern.compile("[0-9]{8}");
		Pattern persIdPattern = Pattern.compile("[a-zA-Z][0-9]{9}");
		if (_id.length() == 8) {
			Matcher matcher = entIdPattern.matcher(_id);
			if (matcher.find()) {
				return true;
			}
		}
		else if (_id.length() == 10) {
			Matcher matcher = persIdPattern.matcher(_id);
			if (matcher.find()) {
				return true;
			}
		}
		return false;
	}
	
	private String encryptID(String _id, DPFTConnector connectorDID) {
		String encID = "";
		try {
			if (_id.length() == 8) {
				_id = "00" + _id + "0";
				DPFTDboSet encryptIDSet = (DPFTDboSet) connectorDID.getDboSet("CMDM.ID_ENC(\'"+_id+"\') AS encID", "DUAL", "");
				encID = encryptIDSet.getDbo(0).getString("encID").trim();
			}
			else if (_id.length() == 10) {
				
				_id = _id.toUpperCase() + "0";
				DPFTDboSet encryptIDSet = (DPFTDboSet) connectorDID.getDboSet("CMDM.ID_ENC(\'"+_id+"\') AS encID", "DUAL", "");
				encID = encryptIDSet.getDbo(0).getString("encID").trim();
			}
		} catch (DPFTRuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return encID;
	}
	
	private void parseJsonToArray(JSONArray ans_array, String[] questions, String[] answers, String[] replyType, int i) {
		JSONObject obj1 = ans_array.getJSONObject(i);
		String type = obj1.getString("type");
		replyType[i] = obj1.getString("replyExternalType");

		// 單行文字
		if (type.equals("single-line-text")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      多行文字
		if (type.equals("multi-line-text")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      單選題
		if (type.equals("single-choice")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      複選題
		if (type.equals("multi-choice")) {
			JSONArray choice_array = obj1.getJSONArray("answer");
			int choice_len = choice_array.length();
			String tempanswer = "";

			for (int j = 0; j < choice_len; j++) {
				JSONObject choicetemp = choice_array.getJSONObject(j);
				Boolean checked = choicetemp.getBoolean("checked");
				if (checked.equals(true)) {
					tempanswer = tempanswer + choicetemp.getString("optionName");
				}
				if (j < choice_len - 1) {
					tempanswer = tempanswer + " | ";
				}
			}

			questions[i] = obj1.getString("name");
			answers[i] = tempanswer;
		}

//      線性評分
		if (type.equals("linear-score")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      下拉式
		if (type.equals("select-choice")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      日期 
		if (type.equals("date-enter")) {
//        	使用getString去讀取obj1內Answer的String
			String answer = obj1.getString("answer");
			JSONObject temp = new JSONObject(answer);
			String dateContent = temp.getString("answer");
			Boolean timeActive = temp.getBoolean("timeActive");

//        	若timeActive為True，抓「日期＋時間」
			if (timeActive) {
				String[] dateAndtime = dateContent.split("T");
				String[] date = dateAndtime[0].split("-");
				String[] time = dateAndtime[1].split(":");
				String datetemp = date[0] + date[1] + date[2] + time[0] + time[1] + time[2].substring(0, 1);
				answers[i] = datetemp;
			}
//        	若timeActive為False，抓「日期」
			else {
				String[] date = dateContent.split("-");
				String datetemp = date[0] + date[1] + date[2];
				answers[i] = datetemp;
			}

//        	將結果加入矩陣中
			questions[i] = obj1.getString("name");

		}

//      時間 
		if (type.equals("time-enter")) {
			String answer = obj1.getString("answer");

			String[] dateAndtime = answer.split("T");
			String[] time = dateAndtime[1].split(":");
			String datetemp = time[0] + time[1] + time[2].substring(0, 2);

//        	將結果加入矩陣中
			answers[i] = datetemp;
			questions[i] = obj1.getString("name");

		}

//      矩陣式
		if (type.equals("matrix-choice")) {

//        	確認answer裡面有幾個row
			int rowNum = obj1.getJSONObject("answer").length();
			String rowsListContent = obj1.getString("rowsList");
			JSONArray rowsList = new JSONArray(rowsListContent);

			String answertemp = "";
			String questiontemp = "";

			for (int j = 0; j < rowNum; j++) {
//        		記錄一題問題及答案
				JSONObject rowNameContent = rowsList.getJSONObject(j);
				String rowName = rowNameContent.getString("rowName");
				String rowAnswer = obj1.getJSONObject("answer").getString("row-" + j);

				questiontemp = questiontemp + rowName;
				answertemp = answertemp + rowAnswer;

//        		用「｜」區隔題目與題目
				if (j < rowNum - 1) {
					answertemp = answertemp + " | ";
					questiontemp = questiontemp + " | ";
				}
			}

//        	將結果加入矩陣中
			answers[i] = answertemp;
			questions[i] = questiontemp;
		}

//      手式選答
		if (type.equals("touch-choice")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      手勢評分
		if (type.equals("touch-score")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getJSONObject("answer").getString("optionName");
		}

//      已詳閱內容並同意
		if (type.equals("check-box")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}

//      固定答案
		if (type.equals("hidden")) {
			questions[i] = obj1.getString("name");
			answers[i] = obj1.getString("answer");
		}
	}
}

package com.ibm.tfb.ext.action;

import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicDataTableWatch;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDbo;
import com.ibm.dpft.engine.core.dbo.DPFTOutboundDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTResMainDbo;
import com.ibm.dpft.engine.core.dbo.DPFTResMainDboSet;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDbo;
import com.ibm.dpft.engine.core.dbo.DPFTTriggerMapDefDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.common.TFBUtil;
import com.ibm.tfb.ext.util.FCBSmsSend;
import com.ibm.tfb.ext.util.FCBSmsTemplate;

public class FCBsmsObndDataTableWatch extends DPFTActionObndPeriodicDataTableWatch {

	@Override
	public String getTableName() {

		return "O_SMS";
	}

	@Override
	public String getTableWatchCriteria() {
		String dateHour = DPFTUtil.getCurrentDateHourAsString();
		StringBuilder sb = new StringBuilder();
		sb.append("concat(SMSDATE,SMSTIME) = '").append(dateHour).append("' and process_status='")
				.append(GlobalConstants.O_DATA_OUTPUT).append("' and response_status is null");
		return sb.toString();
	}

	public String send(DPFTDbo dbo) throws Exception {
		Date now = new Date();
		String nowDate = new SimpleDateFormat("yyyyMMdd").format(now);
		String nowTime = new SimpleDateFormat("HHmmss").format(now);
		FCBSmsSend data = new FCBSmsSend();
		Base64.Encoder encoder = Base64.getEncoder();
		FCBSmsTemplate tmp = new FCBSmsTemplate();
		// NEED TO MAKE SURE COLUMN NAME
		tmp.smsdate = nowDate;
		tmp.smstime = nowTime;
		tmp.purpose = dbo.getString("PURPOSE"); // 原本是放'BIGDATA'
		tmp.category = dbo.getString("CATEGORY"); // 原本是Campaign Code
		tmp.Hostcode = "BIGDATA";
		tmp.brtno = dbo.getString("BRTCODE"); // 原本是'093'
		// tmp.Userid = dbo.getString("USERID"); //身分證號，，原本沒在用
		// tmp.Accno = dbo.getString("ACCNO"); //帳號，原本沒在用
		tmp.Username = dbo.getString("USERNAME"); // 原本沒在用
		tmp.Hostno = dbo.getString("HOSTNO"); // 原本沒在用
		data.UID = dbo.getString("SMSUID");
		; // Test
			// String tmpstr = "004D70860H"; //Test
		String tmpstr = dbo.getString("PWD");
		byte[] strByte = tmpstr.getBytes("UTF-8");
		data.Pwd = encoder.encodeToString(strByte); // Should be 'MDA0RDcwODYwSA=='
		data.DA = dbo.getString("DA");
		data.SM = dbo.getString("SM");
		data.BUSINESSCODE = "";
		data.STOPTIME = "";
		data.AParty = "";
		data.TEMPLATEVAR = URLEncoder.encode(tmp.ToJson(), "UTF-8");
		DPFTLogger.info(this, tmp.ToJson());
		return data.sendService(data);
	}

	@Override
	public void postAction() throws DPFTRuntimeException {
		try {
			DPFTConfig config = DPFTUtil.getSystemDBConfig();
			DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
			DPFTResMainDboSet oSmsRspSet = (DPFTResMainDboSet) connector.getDboSet("RSP_MAIN", "ROWNUM <= 1");
			DPFTDboSet rs = this.getDataSet();
			for (int i = 0; i < rs.count(); i++) {
				try {
					DPFTOutboundDbo smsObDbo = (DPFTOutboundDbo) rs.getDbo(i);
					String sendresponse = send(smsObDbo);
					smsObDbo.setValue("ROW_ID", sendresponse);
					/* Clear personal infos ex. decrypted IDs / decrypted acct Nos after SMS sent */
					smsObDbo.setValue("USERID","");
					smsObDbo.setValue("ACCNO","");
					try {
						int sendresponseint = Integer.parseInt(sendresponse);
						// 1~19 means send to SMS server failed
						if (sendresponseint > 1 && sendresponseint <= 19) {
							DPFTResMainDbo new_dbo = (DPFTResMainDbo) oSmsRspSet.add();
							new_dbo.setValue("CUSTOMER_ID", smsObDbo.getString("CUSTOMER_ID"));
							new_dbo.setValue("TREATMENT_CODE", smsObDbo.getString("TREATMENT_CODE"));
							new_dbo.setValue("CHAL_NAME", smsObDbo.getString("CHAL_NAME"));
							new_dbo.setValue("RES_CODE", "UNDLV");
							String currentTSTMP = DPFTUtil.getCurrentTimeStampAsString();
							new_dbo.setValue("ORIG_RESP_CODE", sendresponse);
							new_dbo.setValue("RES_DATE", currentTSTMP);
							new_dbo.setValue("PROCESS_TIME", currentTSTMP);
							new_dbo.setValue("PROCESS_STATUS", GlobalConstants.DPFT_OBND_STAT_STAGE);
						}
					} catch (NumberFormatException ne) {
						ne.printStackTrace();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			oSmsRspSet.setRefresh(false);
			oSmsRspSet.save();
			oSmsRspSet.close();
			rs.setRefresh(false);
			rs.save();
		} catch (Exception e) {
			DPFTRuntimeException ex = new DPFTRuntimeException("SYSTEM", "DPFT0008E", e);
			ex.handleException();
		}
		DPFTTriggerMapDefDbo tmap = (DPFTTriggerMapDefDbo) this.getInitialData();
		DPFTTriggerMapDefDboSet tmapSet = tmap.getControlTableRecords();
		tmapSet.updateLastActiveTime();
		tmapSet.save();
		tmapSet.close();
		// this.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_COMP);
	}
}

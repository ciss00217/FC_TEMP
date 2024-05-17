package com.ibm.tfb.ext.action;

import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;

import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.action.DPFTActionObndPeriodicDataTableWatch;
import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
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
import com.ibm.dpft.engine.core.util.DPFTUtil;
import com.ibm.tfb.ext.util.FCBSmsSend;

public class FCBsmsResponseDataTableWatch extends DPFTActionObndPeriodicDataTableWatch {

	@Override
	public String getTableName() {
		return "O_SMS";
	}

	@Override
	public String getTableWatchCriteria() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DATE, -3);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		StringBuilder sb = new StringBuilder();
		sb.append("SMSDATE >= '").append(sdf.format(calendar.getTime()))
				.append("' and row_id>'19' or response_status in ('WT','FD','ST','OF') ");
		return sb.toString();
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
					String smsresponse = getSmsResponse(smsObDbo);
					smsObDbo.setValue("RESPONSE_STATUS", smsresponse);
					DPFTResMainDbo new_dbo = (DPFTResMainDbo) oSmsRspSet.add();
					new_dbo.setValue("CUSTOMER_ID", smsObDbo.getString("CUSTOMER_ID"));
					new_dbo.setValue("TREATMENT_CODE", smsObDbo.getString("TREATMENT_CODE"));
					new_dbo.setValue("CHAL_NAME", smsObDbo.getString("CHAL_NAME"));
					if(("OK").equals(smsresponse)) {
						new_dbo.setValue("RES_CODE", "DLV");
					}else{
						new_dbo.setValue("RES_CODE", "UNDLV");
					}
					String currentTSTMP=DPFTUtil.getCurrentTimeStampAsString();
					new_dbo.setValue("ORIG_RESP_CODE", smsresponse);
					new_dbo.setValue("RES_DATE",currentTSTMP );
					new_dbo.setValue("PROCESS_TIME", currentTSTMP);
					new_dbo.setValue("PROCESS_STATUS", GlobalConstants.DPFT_OBND_STAT_STAGE);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			oSmsRspSet.setRefresh(false);
			oSmsRspSet.save();
			oSmsRspSet.close();
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

	public static String getSmsResponse(DPFTDbo dbo) throws Exception {
		FCBSmsSend data = new FCBSmsSend();
		Base64.Encoder encoder = Base64.getEncoder();
		data.UID = dbo.getString("SMSUID");
		// String tmpstr = "004D70860H"; //Test
		String tmpstr = dbo.getString("PWD");
		byte[] strByte = tmpstr.getBytes("UTF-8");
		data.Pwd = encoder.encodeToString(strByte); // Should be 'MDA0RDcwODYwSA=='
		data.DA = dbo.getString("DA");
		data.SM = dbo.getString("SM");
		data.RowID = dbo.getString("ROW_ID");
		return data.responseService(data);
	}
}

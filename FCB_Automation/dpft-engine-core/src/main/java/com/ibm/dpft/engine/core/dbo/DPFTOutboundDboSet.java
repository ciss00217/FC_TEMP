package com.ibm.dpft.engine.core.dbo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class DPFTOutboundDboSet extends DPFTDboSet {
	public DPFTOutboundDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
	}

	private String process_time = null; 
	private HashMap<String, Boolean> gkresult = new HashMap<String, Boolean>();

	
	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new DPFTOutboundDbo(dboname, d, this);
	}

	public DPFTOutboundControlDboSet getControlTableRecords(String addiWhere) throws DPFTRuntimeException {
		String q = buildCtrlQueryString();
		if(addiWhere != null)
			q = q + " and " + addiWhere;
		DPFTLogger.debug(this, "Outbound Records Query String to Ctrl Table:" + q );
		DPFTOutboundControlDboSet oCtrlSet = (DPFTOutboundControlDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("H_OUTBOUND", q);
		oCtrlSet.load();
		return oCtrlSet;
	}

	private String buildCtrlQueryString() throws DPFTRuntimeException {
		StringBuilder cmp_code_list = new StringBuilder();
		StringBuilder t_list = new StringBuilder();
		StringBuilder cell_list = new StringBuilder();
		
		for(int i = 0; i < count(); i++){
			DPFTOutboundDbo dbo = (DPFTOutboundDbo) this.getDbo(i);
			if(!dbo.isNull("camp_code")){
				if(cmp_code_list.indexOf(dbo.getString("camp_code")) == -1)
					cmp_code_list.append("'").append(dbo.getString("camp_code")).append("',");
			}
			if(t_list.indexOf(dbo.getString("timestamp")) == -1)
				t_list.append("'").append(dbo.getString("timestamp")).append("',");
			if(cell_list.indexOf(dbo.getString("cell_code")) == -1)
				cell_list.append("'").append(dbo.getString("cell_code")).append("',");
		}
		
		StringBuilder qString = new StringBuilder();
		if(cmp_code_list.length() > 0){
			qString.append("camp_code in (").append(cmp_code_list.substring(0, cmp_code_list.length()-1)).append(")");
		}else{
			qString.append("camp_code is null");
		}
		if(t_list.length() > 0){
			qString.append(" and timestamp in (").append(t_list.substring(0, t_list.length()-1)).append(")");
		}
		if(cell_list.length() > 0){
			qString.append(" and cell_code in (").append(cell_list.substring(0, cell_list.length()-1)).append(")");
		}
		return qString.toString();
	}
	
	@Override
	public DPFTDbo add() throws DPFTRuntimeException {
		DPFTDbo new_dbo = super.add();
		if(process_time == null)
			process_time = getProcessTimestamp();
		new_dbo.setValue("process_time", process_time);
		return new_dbo;
	}
	
	private String getProcessTimestamp() {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(GlobalConstants.DFPT_DATETIME_FORMAT);
		return sdf.format(date);
	}

	@Override
	public void save() throws DPFTRuntimeException {
		 save(true);
	}

	private void initGKResult() throws DPFTRuntimeException {
		gkresult.clear();
		DPFTDboSet gkresultSet = this.getDBConnector().getDboSet("GK_RESULT", "treatment_code in (" + DPFTUtil.getGKSelectTCodeINString(this) + ")");
		for(int i = 0; i < gkresultSet.count(); i++){
			String key = gkresultSet.getDbo(i).getString("customer_id") + gkresultSet.getDbo(i).getString("treatment_code");
			gkresult.put(key, Boolean.valueOf(true));
		}

		gkresultSet.close();
	}

	private void checkGateKeeper() throws DPFTRuntimeException {
		int count = this.count();
		for(int i = 0; i < count; i++){
			DPFTOutboundDbo dbo = (DPFTOutboundDbo) this.getDbo(i);
			if(!dbo.tobeAdded())
				continue;
			dbo.validateWithGateKeeper(gkresult);
		}
		DPFTLogger.debug(this, "checkGateKeeper done!");
		
	}

	public void save(boolean validate_gk) throws DPFTRuntimeException {
		if(validate_gk){
			if(this.tobeAdded()){
				initGKResult();
			}
				
			checkGateKeeper();
			gkresult.clear();
		}
		super.save();
	}
	public void saveWtGK() throws DPFTRuntimeException {
		super.save();
	}

	public DPFTOutboundControlDboSet getLzControlTableRecords(String addiWhere) throws DPFTRuntimeException {
		String q = buildLzCtrlQueryString();
		if(addiWhere != null)
			q = q + " and " + addiWhere;
		DPFTLogger.debug(this, "Outbound Records Query String to Ctrl Table:" + q );
		DPFTOutboundControlDboSet oCtrlSet = (DPFTOutboundControlDboSet) DPFTConnectionFactory.initDPFTConnector(DPFTUtil.getSystemDBConfig()).getDboSet("H_OUTBOUND", q);
		oCtrlSet.load();
		return oCtrlSet;
	}

	private String buildLzCtrlQueryString() throws DPFTRuntimeException {
		StringBuilder sb = new StringBuilder();
		sb.append("chal_name='").append(this.getDbo(0).getString("chal_name")).append("' and ")
		  .append("timestamp='").append(this.getDbo(0).getString("timestamp")).append("'");
		return sb.toString();
	}

}

package com.ibm.dpft.engine.core.dbo;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.taskplan.DPFTTaskPlan;

public class TaskPlanDefDboSet extends DPFTDboSet {


	public TaskPlanDefDboSet(DPFTConfig conn, String selectAttrs, String tbname, String whereclause)
			throws DPFTRuntimeException {
		super(conn, selectAttrs, tbname, whereclause);
		// TODO Auto-generated constructor stub
	}

	public List<DPFTTaskPlan> convert2TaskPlanList() throws InstantiationException, 
															IllegalAccessException, 
															ClassNotFoundException, 
															IllegalArgumentException, 
															InvocationTargetException, 
															NoSuchMethodException, 
															SecurityException, DPFTRuntimeException{
		if(this.isEmpty())
			return null;
		
		List<DPFTTaskPlan> list = new ArrayList<DPFTTaskPlan>();
		for(int i = 0; i < this.count(); i++){
			TaskPlanDefDbo record = (TaskPlanDefDbo) this.getDbo(i);
			list.add(record.getDefTaskPlanClassInstance());
		}
		return list;
	}
	
	@Override
	protected DPFTDbo getDboInstance(String dboname, HashMap<String, Object> d) {
		return new TaskPlanDefDbo(dboname, d, this);
	}
}

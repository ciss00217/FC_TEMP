package com.ibm.dpft.engine.core.taskplan;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.ibm.dpft.engine.core.DPFTTaskRunner;
import com.ibm.dpft.engine.core.action.DPFTAction;
import com.ibm.dpft.engine.core.common.GlobalConstants;
import com.ibm.dpft.engine.core.dbo.DPFTDbo;
import com.ibm.dpft.engine.core.exception.DPFTActionException;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;

public abstract class DPFTBaseTaskPlan extends DPFTTaskPlan {
	
	List<DPFTAction> actions = new ArrayList<DPFTAction>();
	private String status = GlobalConstants.DPFT_TP_STAT_INIT;
	private DPFTAction current_action = null;
	private int current_action_index = -1;
	private DPFTDbo init_data = null;
	
	public DPFTBaseTaskPlan(String plan_id){
		setId(plan_id);
		setActionsForPlan();
	}
	
	public DPFTBaseTaskPlan(DPFTBaseTaskPlan tp){
		this(tp.getId());
	}
	
	public void finishUp() throws DPFTRuntimeException{
		for(DPFTAction action: actions){
			action.clean();
		}
		actions.clear();
	}
	
	//todo 
	public List<DPFTAction> getAction() {
		return actions;
	}
	
	public void doTask() throws InterruptedException, DPFTRuntimeException{
		changeTaskStatus(GlobalConstants.DPFT_TP_STAT_EXEC);
		while(!status.equals(GlobalConstants.DPFT_TP_STAT_COMP)){
			try {
				doActions();
			} catch (Exception e) {
				if(e instanceof DPFTRuntimeException)
					((DPFTRuntimeException)e).handleException();
				else
					throw new DPFTRuntimeException("SYSTEM", "DPFT0003E", e);
			}
		}
	}
	
	private void doActions() throws InterruptedException, DPFTRuntimeException {
		
		if(actions.isEmpty()){
			DPFTLogger.info(this, "No Action for execution...");
			changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
			return;
		}
			
		if(current_action == null){
			current_action = getNextActionFromList();
			if(init_data != null)
				current_action.setInitialData(init_data);
		}else{
			if(init_data != null)
				current_action.setInitialData(init_data);
			
			/*validate whether current action is complete*/
			if(current_action.isActionComplete()){
				/*allow current action to perform finishing job*/
				current_action.finish();
				if(hasNextAction()){
					/*action complete set current action to next action in actions list*/
					DPFTAction prev_action = current_action;
					current_action = getNextActionFromList();
					current_action.setPreviousAction(prev_action);
				}else{
					if(isRecurring()){
						resetAction();
						DPFTLogger.info(this, "Task Plan Recurring...");
					}else{
						DPFTLogger.info(this, "Task Plan Complete...");
						changeTaskStatus(GlobalConstants.DPFT_TP_STAT_COMP);
						return;
					}
				}
			}
		}
		current_action.changeActionStatus(GlobalConstants.DPFT_ACTION_STAT_RUN);
		try{
			current_action.action();
		}catch(Exception e){
			if(e instanceof DPFTActionException)
				throw e;
			throw new DPFTActionException(current_action, "SYSTEM", "DPFT0008E", e);
		}
		
		long sleep_time = current_action.getPostActionSleepTime();
		if(sleep_time > 0){
			DPFTTaskRunner.sleep(sleep_time);
		}
	}

	private void resetAction() throws DPFTRuntimeException {
		
		/*Reset Actions list and set current action to the first one*/
		finishUp();
		setActionsForPlan();
		current_action_index = -1;
		current_action = getNextActionFromList();
	}

	private boolean hasNextAction() {
		
		return (current_action_index + 1 == actions.size())?false:true;
	}

	private DPFTAction getNextActionFromList() {
		
		return actions.get(++current_action_index);
	}

	protected void changeTaskStatus(String stat) {
		
		status = stat;
	}

	public List<DPFTAction> getActionList(){
		return actions;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public DPFTBaseTaskPlan getNewInstance() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		
		return (DPFTBaseTaskPlan) Class.forName(this.getClass().getName())
		.getConstructor(DPFTBaseTaskPlan.class)
		.newInstance(this);
	}

	public DPFTDbo getInitialDataSet() {
		return init_data;
	}

	public void setInitialDataSet(DPFTDbo dbo) {
		this.init_data = dbo;
	}
}

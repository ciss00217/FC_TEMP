package com.ibm.tfb.ext.common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.ibm.dpft.engine.core.action.DPFTActionTableWatch;
import com.ibm.dpft.engine.core.config.DPFTConfig;
import com.ibm.dpft.engine.core.connection.DPFTConnectionFactory;
import com.ibm.dpft.engine.core.connection.DPFTConnector;
import com.ibm.dpft.engine.core.dbo.DPFTDboSet;
import com.ibm.dpft.engine.core.exception.DPFTRuntimeException;
import com.ibm.dpft.engine.core.util.DPFTLogger;
import com.ibm.dpft.engine.core.util.DPFTUtil;

public class CalendarUtil {
	//Global
	Calendar calendar = Calendar.getInstance();
	final static String[] dayOfWeek = { "", "Sun", "Mon", "Tue", "Wed", "Tur", "Fri", "Sat" };		//Day of week definition
	private List<String> holidays = new ArrayList<String>();
	
	public CalendarUtil() throws DPFTRuntimeException {
		this(new Date());
	}
	
	public CalendarUtil(Date date) throws DPFTRuntimeException {
		init(date);
	}
	
	/**
	* 取得當前週次
	* 
	* @param
	* @return 回傳週次為integer型態
	*/
	//H0.03 Add
	public static int getCurrentWeekOfYear() {
		Date current_date = new Date();
		Calendar cal = Calendar.getInstance();
		cal.setTime(current_date);
		return cal.get(Calendar.WEEK_OF_YEAR);
	}

	private void init(Date date) throws DPFTRuntimeException {
		
		calendar.setTime(date);
		if(holidays.size() == 0){
			//init holiday list
			holidays = getListOfHolidays(getYear());
		}
		addHolidaysFromLocalDB();
	}

	private void addHolidaysFromLocalDB() throws DPFTRuntimeException {
		
		StringBuilder sb = new StringBuilder();
		sb.append("FLAG = 'N'");
		
		
		DPFTConfig config = DPFTUtil.getSystemDBConfig();
		DPFTConnector connector = DPFTConnectionFactory.initDPFTConnector(config);
		DPFTDboSet hSet =  connector.getDboSet("CALENDAR", sb.toString());
		hSet.load();
		
		//MboSetRemote hSet = MXServer.getMXServer().getMboSet("PX_RMHOLIDAY");
		int count = hSet.count();
		for(int i = 0; i < count; i++){
			Calendar cal = Calendar.getInstance();
			cal.setTime(hSet.getDbo(i).getDate("calendar_date"));
			String date = getDate(cal);
			//H0.01 Start
			//String type = hSet.getDbo(i).getString("type");
			addHoliday(date);
			/*if(type.equals(GlobalConstants.PX_DAYTYPE_HOLIDAY)){
				//add holiday to cache holiday list
				addHoliday(date);
			}else{
				//specified work day, remove from holiday list
				removeHoliday(date);
			}*/
			//H0.01 End
//H0.01			if(!holidays.contains(date)){
//H0.01				//add holiday to cache holiday list
//H0.01				addHoliday(date);
//H0.01			}
		}
	}

	private List<String> getListOfHolidays(int year) {
		
		Calendar cal = Calendar.getInstance();
		int d = 1;
		List<String> holidayList = new LinkedList<String>();
		while (true) {
			cal.clear();
			cal.set(year, 0, d);
			getYear(cal);
			if (getYear(cal) != year) {
				break;
			}
			if (isHoliday(cal)) {
				holidayList.add(getDate(cal));
			}
			d++;
		}
		System.out.println(year + "年今年共 " + (d - 1) + "天");
		return holidayList;
	}
	
	public int getYear(Calendar cal) {
		return cal.get(Calendar.YEAR);
		}

	public int getYear() {
		return calendar.get(Calendar.YEAR);
	}
	
	/**
	* 取得calendar的日期
	* 
	* @param Calendar
	* cal
	* @return 回傳 yyyy/mm/dd 格式
	*/
	private String getDate(Calendar cal) {
		return getYear(cal) + "/" + getMonth(cal) + "/" + getDayOfMonth(cal);
	}
	
	/**
	* 取得calendar的日期
	*
	* @return 回傳 yyyy/mm/dd 格式
	*/
	public String getDate() {
		return getYear(calendar) + "/" + getMonth(calendar) + "/" + getDayOfMonth(calendar);
	}
	
	public int getMonth(Calendar cal) {
		return cal.get(Calendar.MONTH) + 1;
	}
	
	public int getDayOfMonth(Calendar cal) {
		return cal.get(Calendar.DAY_OF_MONTH);
	}
	
	/**
	* 判斷是否為例假日
	* 
	* @param cal
	* @return
	*/
	public boolean isHoliday(Calendar cal) {

		if (null == cal) {
			return false;
		}
		// 1.六日
		String dayofweek = getDayOfWeek(cal);
		if (dayofweek.equals("Sat") || dayofweek.equals("Sun")) {
			return true;
		}

		// 2.國定假日
		if (holidays.contains(getDate(cal))) {
			return true;
		}

		return false;
	}
	
	public boolean isHoliday() {
//H0.01		return isHoliday(calendar);
		//H0.01 Start
		if (null == calendar) {
			return false;
		}
		if (holidays.contains(getDate(calendar))) {
			return true;
		}
		return false;
		//H0.01 End
	}

	
	/**
	* 判斷是星期幾
	* 
	* @param cal
	* @return
	*/
	public String getDayOfWeek(Calendar cal) {
		if (cal == null) {
			return dayOfWeek[calendar.get(Calendar.DAY_OF_WEEK)];
		}
		return dayOfWeek[cal.get(Calendar.DAY_OF_WEEK)];
	}
	
	public void addHoliday(String date) {
		if (!holidays.contains(date)) {
		this.holidays.add(date);
		}
	}

	public void removeHoliday(String date) {
		this.holidays.remove(date);
	}
	
	public Calendar getAfterDate(int offset) {
		for(int i = 1; i < offset; i++){
			getNextWorkingDate();
		}
		return calendar;
	}
	
	public Calendar getEarlierDate(int offset) {
		for(int i = 1; i < offset; i++){
			getPrevWorkingDate();
		}
		return calendar;
	}
	
	public Calendar getNextWorkingDate() {
		while(true){
			calendar.add(Calendar.DAY_OF_MONTH, 1);
			if(!isHoliday()){
				return calendar;
			}
		}
	}
	
	public Calendar getPrevWorkingDate() {
		while(true){
			calendar.add(Calendar.DAY_OF_MONTH, -1);
			if(!isHoliday()){
				return calendar;
			}
		}
	}
	
	public Date getWeekStartDate(){
		while(!getDayOfWeek(calendar).equals("Sun")){
			calendar.add(Calendar.DAY_OF_MONTH, -1);
		}
		return calendar.getTime();
	}
	
	public Date getWeekEndDate(){
		while(!getDayOfWeek(calendar).equals("Sat")){
			calendar.add(Calendar.DAY_OF_MONTH, 1);
		}
		return calendar.getTime();
	}
	
	public void setDate(Date date) {
		calendar.setTime(date);
	}
	
	public String CalendarToString(){
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return format.format(calendar.getTime());
	}
	
	public Date CalendarToDate(){
		return calendar.getTime();
	}
	
/*	*//**
	* 回傳假日描述
	* 
	* @return 
	*//*
	public String getHolidayDescription(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		//Remove time parts
		cal.set(Calendar.HOUR_OF_DAY, 0);						//H0.02
		cal.set(Calendar.MINUTE, 0);							//H0.02
		cal.set(Calendar.SECOND, 0);							//H0.02
		cal.set(Calendar.MILLISECOND, 0);						//H0.02
		if(!isHoliday(cal))
			return null;
		
		date = cal.getTime();									//H0.02
		StringBuilder sb = new StringBuilder();
		MboSetRemote hSet = MXServer.getMXServer().getMboSet("PX_RMHOLIDAY");
		int count = hSet.count();
		for(int i = 0; i < count; i++){
			if(date.equals(hSet.getMbo(i).getDate("date"))){
				sb.append(hSet.getMbo(i).getString("description"));
			}
		}
		if(sb.length() == 0){
			if(getDayOfWeek(cal).equals("Sat"))
				sb.append("星期六");
			else
				sb.append("星期日");
		}
		return sb.toString();
	}*/
}

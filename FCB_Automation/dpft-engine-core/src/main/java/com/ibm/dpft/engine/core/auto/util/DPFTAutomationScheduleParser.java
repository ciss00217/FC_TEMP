package com.ibm.dpft.engine.core.auto.util;

import java.util.Calendar;

import com.ibm.dpft.engine.core.util.DPFTLogger;

public class DPFTAutomationScheduleParser {
	private boolean IsScheduled = false;

    public DPFTAutomationScheduleParser(String SMinute, String SHour, String SDayOfMonth, String SMonth, String SDayOfWeek){
    	DPFTLogger.debug(this, "[Info] [JIsScheduledJob] SMinute = " + SMinute + ", SHour = " + SHour);
        DPFTLogger.debug(this, "[Info] [JIsScheduledJob] SDayOfMonth = " + SDayOfMonth + ", SMonth = " + SMonth + ", SDayOfWeek = " + SDayOfWeek);
//        if (CheckTime(SMinute, SHour) && CheckDayOfMonth(SDayOfMonth) &&
        if (CheckDayOfMonth(SDayOfMonth) &&
        		CheckMonth(SMonth) && CheckDayOfWeek(SDayOfWeek)) 
        IsScheduled = true;
        DPFTLogger.debug(this, "[Info] [JIsScheduledJob] IsScheduled = " + IsScheduled);
    }

    @SuppressWarnings("unused")
	private boolean CheckTime (String SMinute, String SHour){
    	DPFTLogger.debug(this, "[Info] [JIsScheduledJob] Into CheckTime");
    	DPFTLogger.debug(this, "[Info] [JIsScheduledJob] SMinute = " + SMinute + ", SHour = " + SHour);
        boolean ReturnFlag = false;
        if (SHour.indexOf("/") != -1)
        	ReturnFlag = true;
        else{
            Calendar cal = Calendar.getInstance();
            int NowTime = cal.get(Calendar.HOUR_OF_DAY) * 100 + cal.get(Calendar.MINUTE);
            int ScheduledTime = Integer.valueOf(SHour.replace(" ", "")) * 100 + Integer.valueOf(SMinute.replace(" ", ""));
            DPFTLogger.debug(this, "[Info] [JIsScheduledJob] ScheduledTime = " + ScheduledTime + ", NowTime = " + NowTime);
            if (ScheduledTime <= NowTime) ReturnFlag = true;
        }
        DPFTLogger.debug(this,"[Info] [JIsScheduledJob] ReturnFlag = " + ReturnFlag);
        DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Exit CheckTime");
        return ReturnFlag;
    }

    private boolean CheckDayOfMonth (String DMonth) {
    	DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Into CheckDayOfMonth");
    	DPFTLogger.debug(this,"[Info] [JIsScheduledJob] DMonth = " + DMonth);
        boolean ReturnFlag = false;
        Calendar cal = Calendar.getInstance();
        int DayOfMonth = cal.get(Calendar.DAY_OF_MONTH);
        if (DMonth.indexOf("*") != -1 || DMonth.indexOf("?") != -1)
       	    ReturnFlag = true;
        else if (DMonth.indexOf(",") != -1){
            String Days[] = DMonth.split(",");
            for (int i = 0; i < Days.length; i ++)
            	if (DayOfMonth == Integer.valueOf(Days[i].replace(" ", ""))) 
            		ReturnFlag = true;
        }else if (DMonth.indexOf("-") != -1){
            String Days[] = DMonth.split("-");
            if (DayOfMonth >= Integer.valueOf(Days[0].replace(" ", "")) && DayOfMonth <= Integer.valueOf(Days[1].replace(" ", ""))) 
            	ReturnFlag = true;
        }else if (DayOfMonth == Integer.valueOf(DMonth.replace(" ", "")))
        	ReturnFlag = true;
        DPFTLogger.debug(this,"[Info] [JIsScheduledJob] ReturnFlag = " + ReturnFlag);
        DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Exit CheckDayOfMonth");
        return ReturnFlag;
    }

    private boolean CheckMonth (String CMonth){
    	DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Into CheckMonth");
    	DPFTLogger.debug(this,"[Info] [JIsScheduledJob] CMonth = " + CMonth);
        boolean ReturnFlag = false;
        Calendar cal = Calendar.getInstance();
        int CurrentMonth = cal.get(Calendar.MONTH) + 1;
        String Months = new String("JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC");
        	if (CMonth.indexOf("*") != -1)
        		ReturnFlag = true;
        	else if (CMonth.indexOf(",") != -1){
        		// Modified by Don - 2011/05/19. Add "JAN,FEB,MAY or JAN-MAY" criteria.
                // Modification Start.
                String Mon[] = CMonth.split(",");
                for (int i = 0; i < Mon.length; i ++){
                	int MonIndex = Months.indexOf(Mon[i].toUpperCase());
                    if (MonIndex != -1){
                    	if (CurrentMonth == ((MonIndex / 4) + 1)) 
                    		ReturnFlag  = true;
                    }else{
                        if (CurrentMonth == Integer.valueOf(Mon[i].replace(" ", ""))) 
                        	ReturnFlag  = true;
                    }
                    if (ReturnFlag) 
                    	break;
                 }
                 // Modification End.
            }
        	else if (CMonth.indexOf("-") != -1){
        		String Mon[] = CMonth.split("-");
                int Mon1 = Months.indexOf(Mon[0].toUpperCase());
                int Mon2 = Months.indexOf(Mon[1].toUpperCase());
                if (Mon1 != -1 && Mon2 != -1){
                	if (CurrentMonth >= ((Mon1 / 4) + 1) && CurrentMonth <= ((Mon2 / 4) + 1)) 
                		ReturnFlag  = true;
                }else{
                    if (CurrentMonth >= Integer.valueOf(Mon[0].replace(" ", "")) && CurrentMonth <= Integer.valueOf(Mon[1].replace(" ", ""))) 
                    	ReturnFlag  = true;
                    }
            }else{
                int Mon = Months.indexOf(CMonth.toUpperCase());
                if (Mon != -1){
                	if (CurrentMonth == ((Mon / 4) + 1)) ReturnFlag = true;
                }else if (CurrentMonth == Integer.valueOf(CMonth.replace(" ", "")))
                	ReturnFlag = true;
                }
        DPFTLogger.debug(this,"[Info] [JIsScheduledJob] ReturnFlag = " + ReturnFlag);
        DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Exit CheckMonth");
        return ReturnFlag;
    }

    private boolean CheckDayOfWeek (String CDay){
    	DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Into CheckDayOfWeek");
    	DPFTLogger.debug(this,"[Info] [JIsScheduledJob] CDay = " + CDay);
        boolean ReturnFlag = false;
        Calendar cal = Calendar.getInstance();
        int DayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
        String Weeks = new String("SUN,MON,TUE,WED,THU,FRI,SAT");
        	if (CDay.indexOf("*") != -1 || CDay.indexOf("?") != -1)
        		ReturnFlag = true;
            else if (CDay.indexOf(",") != -1){
            	// Modified by Don - 2011/05/19. Add "MON,TUE,WED,THU,FRI/1" criteria.
            	// Modification Start.
            	String DayW[] = CDay.split(",");
            	for (int i = 0; i < DayW.length; i ++){
            		int DayWIndex = -1;
            		if (DayW[i].length() >= 3) DayWIndex = Weeks.indexOf(DayW[i].toUpperCase().substring(0, 3));
                    if (DayWIndex != -1){
                    	if (DayOfWeek == ((DayWIndex / 4) + 1)) ReturnFlag  = true;
                    }else{
                        String DayWW = DayW[i].replace(" ", "");
                        int SIndex = DayWW.indexOf("/");
                        if (SIndex == -1) SIndex = 1;
                        if (DayOfWeek == Integer.valueOf(DayWW.substring(0, SIndex))) ReturnFlag  = true;
                    }
                    if (ReturnFlag) break;
                }
                // Modification End.
            }else if (CDay.indexOf("-") != -1){
            	String DayW[] = CDay.split("-");
                int DayW1 = Weeks.indexOf(DayW[0].toUpperCase());
                int DayW2 = Weeks.indexOf(DayW[1].toUpperCase());
                if (DayW1 != -1 && DayW2 != -1){
                        if (DayOfWeek >= ((DayW1 / 4) + 1) && DayOfWeek <= ((DayW2 / 4) + 1)) ReturnFlag  = true;
                }else{
                        if (DayOfWeek >= Integer.valueOf(DayW[0].replace(" ", "")) && DayOfWeek <= Integer.valueOf(DayW[1].replace(" ", ""))) ReturnFlag  = true;
                }
            }else{
            	int Day = Weeks.indexOf(CDay.toUpperCase());
                if (Day != -1){
                	if (DayOfWeek == ((Day / 4) + 1)) ReturnFlag = true;
                }else if (DayOfWeek == Integer.valueOf(CDay.replace(" ", "")))
                    ReturnFlag = true;
            }
       DPFTLogger.debug(this,"[Info] [JIsScheduledJob] ReturnFlag = " + ReturnFlag);
       DPFTLogger.debug(this,"[Info] [JIsScheduledJob] Exit CheckDayOfWeek");
       return ReturnFlag;
    }

    public boolean IsScheduled (){
       return IsScheduled;
    }
}

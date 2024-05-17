package com.ibm.dpft.engine.core.common;

public class GlobalConstants {

	public static final String DB_ORACLE = "ORACLE";
	public static final String DB_MSSQL = "MSSQL";
	public static final String SYSTEM_CMD_EXIT = "EXIT";
	public static final String MSG_TITLE_01 = "DPFT Engine";
	public static final String MSG_MAIL_TITLE_01 = "[行銷平台 通路整合自動化平台訊息]";
	public static final String Hyphen = "-";
	public static final String DFPT_DATETIME_FORMAT = "yyyyMMddHHmmss";
	public static final String DFPT_DATETIME_FORMAT_2 = "yyyyMMddHHmm";
	public static final String DFPT_DATETIMEHOUR_FORMAT     = "yyyyMMddHH";
	public static final String DFPT_DATE_FORMAT     = "yyyyMMdd";
	public static final String DPFT_MONTH_FORMAT    = "yyyyMM";
	public static final String DPFT_DEFAULT_PRIORITY_CODE = "00";
	public static final String ORA_DB_TIME_FORMAT   = "YYYYMMDDHH24MISS";
	public static final String DFPT_YYMMDD_FORMAT     = "yyMMdd";
	public static final String DFPT_HHMMSS_FORMAT     = "HHmmss";
	
	public static final int    LOG_LEVEL_INFO 	= 1;
	public static final int    LOG_LEVEL_DEBUG  = 2;
	public static final int    LOG_LEVEL_ERROR 	= 3;
	
	/*Database connection profile definition*/
	public static final String DB_CONN_PROFILE_SYS      = "sys";
	public static final String DB_CONN_PROFILE_MKTDM    = "CMDM";
	public static final String DB_CONN_PROFILE_UNICAMPP = "BDINMP";
	public static final String DB_CONN_PROFILE_UNICADBP = "BDINCMP";
	
	/*Engine stat definition*/
	public static final String DPFT_ENGINE_STAT_INIT    = "INIT";
	public static final String DPFT_ENGINE_STAT_RUN     = "RUN";
	public static final String DPFT_ENGINE_STAT_PASSIVE = "PASSIVE";
	
	/*DB statement type define*/
	public static final String DB_STMT_TYPE_QUERY = "Q";
	public static final String DB_STMT_TYPE_UPDATE= "U";
	public static final String DB_STMT_TYPE_INSERT= "I";
	public static final String DB_STMT_TYPE_DELETE= "D";
	public static final String DB_STMT_TYPE_SQL   = "S";
	
	/*Engine Daemon thread ID Definition*/
	public static final String DPFT_SYS_UNICA_RUNNER_ID 	= "DPFT01";
	public static final String DPFT_SYS_SCHEDULE_RUNNER_ID	= "DPFT02";
	public static final String DPFT_SYS_OBND_RUNNER_ID      = "DPFT03";
	public static final String DPFT_SYS_OBND_PERIODIC_RUNNER_ID = "DPFT04";
	public static final String DPFT_SYS_RES_GETFILE_RUNNER_ID = "DPFT05";
	public static final String DPFT_SYS_RES_READFILE_RUNNER_ID = "DPFT06";
	public static final String DPFT_SYS_RES_INSTA_FILETRIGGER_RUNNER_ID = "DPFT07";
	public static final String DPFT_SYS_RES_INSTA_SYS_SCHEDULE_RUNNER_ID = "DPFT08";
	public static final String DPFT_SYS_RES_INSTA_SYS_OBND_RUNNER_ID = "DPFT09";
	public static final String DPFT_SYS_MAIL_RUNNER_ID = "DPFT10";
	
	
	/*DPFT task plan status definition*/
	public static final String DPFT_TP_STAT_INIT =  "INIT";
	public static final String DPFT_TP_STAT_EXEC  = "EXEC";
	public static final String DPFT_TP_STAT_COMP =  "COMP";
	
	/*DPFT task plan Out file rule definition*/
	public static final String DPFT_TP_OBND_RULE_CAMP = "CAMP";
	public static final String DPFT_TP_OBND_RULE_CELL = "CELL";
	public static final String DPFT_TP_OBND_RULE_CHAL = "CHAL";
	
	/*DPFT task plan id pattern keyword definition*/
	public static final String DPFT_TP_KEY_PSV        = "PSV";
	
	/*DPFT Outbound task plan frequency definition*/
	public static final String DPFT_OBND_FEQ_HOURLY    = "HOURLY";
	public static final String DPFT_OBND_FEQ_DAILY    = "DAY";
	public static final String DPFT_OBND_FEQ_MONTHLY  = "MONTH";
	public static final String DPFT_OBND_FEQ_YEARLY   = "YEAR";
	public static final String DPFT_OBND_TT_MONTH_ENDDAY    = "ED";
	public static final String DPFT_OBND_TT_MONTH_FIRSTDAY  = "FD";
	
	/*DPFT action status definition*/
	public static final String DPFT_ACTION_STAT_INIT = "INIT";
	public static final String DPFT_ACTION_STAT_RUN  = "RUN";
	public static final String DPFT_ACTION_STAT_COMP = "COMP";
	
	/*Control table process status definition*/
	public static final String DPFT_CTRL_STAT_INIT = "I";
	public static final String DPFT_CTRL_STAT_RUN  = "R";
	public static final String DPFT_CTRL_STAT_COMP = "C";
	public static final String DPFT_CTRL_STAT_ERROR= "E";
	
	public static final String DPFT_OBND_STAT_STAGE= "S";
	public static final String DPFT_OBND_STAT_RUN  = "R";
	public static final String DPFT_OBND_STAT_FIN  = "F";
	public static final String DPFT_OBND_STAT_ERROR= "E";
	
	/*File Delimeter definition*/
	public static final String FILE_DELIMETER_COMMA = ",";
	public static final String FILE_DELIMETER_COMMA_FULL = "，";
	public static final String FILE_DELIMETER_TAB   = "\t";
	public static final String FILE_DELIMETER_AND   = "&";
	public static final String FILE_DELIMETER_SEMICOLON = ";";
	public static final String FILE_DELIMETER_SEMICOLON_FULL = "；";
	public static final String FILE_DELIMETER_DOT   = "\\.";
	public static final String FILE_DELIMETER_SHARP = "#";
	public static final String FILE_DELIMETER_PIP   = "\\|\\|";
	public static final String FILE_DELIMETER_SPIP  = "|";
	public static final String FILE_DELIMETER_ESPIP  = "\\|";
	public static final String FILE_DELIMETER_SPACE  = "\\s+";
	public static final String FILE_DELIMETER_EMPTY  = "";
	public static final String FILE_EOL             = "\r\n";
	public static final String FILE_TRF_CNST        = "QUANTITY=";
	public static final String FILE_STATIC_LEN_UNIT_CHAR = "CHAR";
	public static final String FILE_STATIC_LEN_UNIT_BYTE = "BYTE";
	
	/*Control File definition*/
	public static final String[] H_HEADER = {"ACTION", "QUANTITY"};
	public static final String[] H_HEADER2 = {"QUANTITY"};
	public static final String H_FILE_ACTION_INSERT = "I";
	
	/*outbound data stat definition*/
	public static final String O_DATA_EXCLUDE    = "Exclude";
	public static final String O_DATA_GK_EXCLUDE = "Exclude_GK";
	public static final String O_DATA_OUTPUT     = "Output";
	public static final String O_DATA_DUMMY      = "Dummy";
	public static final String O_DATA_EXPIRE_EXCLUDE = "Exclude_EX";
	public static final String O_DATA_MGM_EXCLUDE = "Exclude_M";
	
	
	/*File meta constants*/
	public static final String FILE_META_ID_H_FILE      = "H_FILE";
	public static final String FILE_META_ID_D_FILE      = "D_FILE";
	public static final String FILE_META_ID_LOCAL_DIR   = "LOC_DIR";
	public static final String FILE_META_ID_FTP_DIR     = "FTP_DIR";
	public static final String FILE_META_ID_FTP_DIR_1     = "FTP_DIR_1";
	public static final String FILE_META_ID_ENCODE      = "FILE_ENCODE";
	public static final String FILE_META_ID_STATIC_MODE = "STATIC_MODE";
	public static final String FILE_META_ID_HEADER      = "HEADER";
	public static final String FILE_META_ID_TRF_VALID   = "TRF_VALID";
	public static final String FILE_META_ID_TRF_CNST   = "TRF_CNST";
	public static final String FILE_META_ID_STATIC_TRF_LENGTH = "STATIC_TRF_LENGTH";
	public static final String FILE_META_ID_COMPRESS    = "COMPRESS";
	public static final String FILE_META_ID_COMPRESS_FILE = "COMPRESS_FILE";
	public static final String FILE_META_ID_Z_FILE      = "Z_FILE";
	public static final String FILE_META_ID_H_FILE_ACT_ID = "H_FILE_ACT_ID";
	public static final String FILE_META_ID_CD_PROFILE  = "CD_PROFILE";
	public static final String FILE_META_ID_STATIC_LEN_UNIT = "STATIC_LEN_UNIT";
	public static final String FILE_META_ID_DELIMETER   = "DELIMETER";
	
	
	/*File Encode Constants*/
	public static final String FILE_ENCODE_UTF8         = "UTF-8";
	public static final String FILE_ENCODE_BIG5         = "MS950";
	
	/*File name pattern constants*/
	public static final String FILE_PATTERN_CNST_SYSDATE     = "$SYSDATE";
	public static final String FILE_PATTERN_CNST_SYSDATETIME = "$SYSDATETIME";
	public static final String FILE_PATTERN_CNST_STAR        = "*";
	public static final String FILE_PATTERN_CNST_SYSPROP     = "$SYSPROP";
	public static final String FILE_PATTERN_CNST_YYMMDD     = "$YYMMDD";
	public static final String FILE_PATTERN_CNST_HHMMSS     = "$HHMMSS";
	
	/*FTP Constants*/
	public static final int FTP_MODE_BINARY = 1;
	
	public static final int ERROR_LEVEL_TRF_FAILURE = 8;
	public static final int ERROR_LEVEL_READ_FAILURE = 4;
	public static final int ERROR_LEVEL_TRF_SUCCESS = 0;
	
	/*Response file Constants*/
	public static final String DPFT_DEFAULT_RES_LAYOUT  = "RES_CTRL";
	
	/*file reader Constants*/
	public static final String DPFT_READER_DS_FILE = "FILE";
	public static final String DPFT_READER_DS_TBL  = "TBL";
	
	/*System properties Key definition*/
	public static final String DPFT_SYS_PROP_AUTO_ENABLE_TIME		= "sys.engine.auto.time.enabled";
	public static final String DPFT_SYS_PROP_AUTO_ENABLE     		= "sys.engine.auto.enabled";
	public static final String DPFT_SYS_PROP_AUTO_MACRO_CMD_LOGPATH = "sys.engine.auto.macro.cmd.logpath";
	public static final String DPFT_SYS_PROP_CONN_RETRY 			= "sys.connection.retry";
	
	/*Automation Constants definition*/
	public static final String DPFT_AUTOMATION_TBL_STR = "Automation.TBL";
	public static final String DPFT_AUTOMATION_PS_MGR_STR = "Automation.TPID.ProcessManager";
	public static final String DPFT_AUTOMATION_PS_MGR_ID  = "AUTO_MGR";
	public static final String DPFT_AUTOMATION_EXECUTOR_TPID = "AUTO_EXEC";
	public static final String DPFT_AUTOMATION_PROCESS_ACTION_EXEC = "Execute";
	public static final String DPFT_AUTOMATION_PS_RUN_ID_LOCAL = "localhost";
	
	/*Automation Process Status definition*/
	public static final String DPFT_AUTOMATION_PROCESS_STATUS_INIT     = "INIT";
	public static final String DPFT_AUTOMATION_PROCESS_STATUS_RUNNABLE = "RUNNABLE";
	public static final String DPFT_AUTOMATION_PROCESS_STATUS_RUNNING  = "RUNNING";
	public static final String DPFT_AUTOMATION_PROCESS_STATUS_FIN      = "FINISHED";
	public static final Object DPFT_AUTOMATION_PROCESS_STATUS_ERROR    = "ERROR";
	
	/*Automation Execution Return Code definition*/
	public static final int DPFT_AUTOMATION_PS_RC_NORMAL  = 0;
	public static final int DPFT_AUTOMATION_PS_RC_WAITING = 1;
	public static final int DPFT_AUTOMATION_PS_RC_ERROR   = 8;
	public static final int DPFT_AUTOMATION_PS_RC_TRUE    = 0;
	public static final int DPFT_AUTOMATION_PS_RC_FALSE   = 8;
	
	/*Automation MACRO Constants*/
	public static final String DPFT_AUTOMATION_MACRO_CONDITION = "CND";
	public static final String DPFT_AUTOMATION_MACRO_VARS      = "VAR";
	public static final String DPFT_AUTOMATION_MACRO_VARS_RS   = "RS";
	
	/*Automation MACRO Action Constants*/
	public static final String DPFT_AUTOMATION_MACRO_ACTION_CHKCND = "processCondition";
	
	/*Automation Flowchart CCT Status*/
	public static final String DPFT_AUTOMATION_FLOWCHAR_RUN_SUCCESS = "Success";
	public static final String DPFT_AUTOMATION_FLOWCHAR_RUN_FAILED  = "Failed";
	public static final String DPFT_AUTOMATION_FLOWCHAR_RUN_CMD_FAILED = "Init_Failed";
	public static final String DPFT_AUTOMATION_FLOWCHAR_RUN_INPRG   = "In Progress";
	
	/*Automation Flowchart Type definition*/
	public static final String DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_C = "C";  
	public static final String DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_G = "G";		//GK
	public static final String DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_R = "R";
	public static final String DPFT_AUTOMATION_UNICA_FLOWCHART_TYPE_RP = "RP";
	
	

}

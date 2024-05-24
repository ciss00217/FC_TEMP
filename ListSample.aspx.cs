using gbs.bao.etl.bo;
using gbs.bao.etl.entity;
using NLog;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace ETLAdm.accessories
{
	public partial class ListSample : System.Web.UI.Page
	{
		private Logger _log = LogManager.GetCurrentClassLogger();
		protected void Page_Load(object sender, EventArgs e)
		{
			if (MarketingActtivityList.Items.Count == 1)
			{
				string loginID = (string)Session["User"];
				_log.Debug("loginID: " + loginID);
				int loginIDInt = 0;
				_log.Debug("loginIDInt: " + loginIDInt);
				Boolean tryParseLoginIDInt = int.TryParse(loginID.Substring(1), out loginIDInt);
				_log.Debug("tryParseLoginIDInt" + tryParseLoginIDInt);
				_log.Debug("loginIDInt" + loginIDInt);
				if (tryParseLoginIDInt)
				{
					string strConn = getCMAUTOConnString();// "server=.\\SQLExpress;database=databaseName;User ID=username;Password=password;Trusted_Connection=True;";
					using (OracleConnection connection = new OracleConnection(strConn))
					{
						_log.Debug("OracleConnection:"+ strConn);
		
						connection.Open();
						//Console.WriteLine("State: {0}", connection.State);
						//Console.WriteLine("ConnectionString: {0}", connection.ConnectionString);

						OracleCommand activityCommand = connection.CreateCommand();
						String activitySql = "Select DISTINCT (CAMP_CODE||CAMPAIGNNAME),CAMP_CODE,CAMPAIGNNAME From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER ='" + loginID.Substring(0,3) + "'";// 
						_log.Debug("activitySql:" + activitySql);
						activityCommand.CommandText = activitySql;
						OracleDataReader activityReader = activityCommand.ExecuteReader();
						_log.Debug("activityReader.HasRows:" + activityReader.HasRows);
						while (activityReader.Read())
						{
							ListItem listItem = new ListItem();
							listItem.Text = activityReader["CAMPAIGNNAME"].ToString();
							listItem.Value = activityReader["CAMP_CODE"].ToString();
							_log.Debug("listItem.Text" + listItem.Text);
							MarketingActtivityList.Items.Add(listItem);
						}
						OracleCommand audienceCommand = connection.CreateCommand();
						String audienceSql = "Select DISTINCT (CELL_CODE||CELLNAME),CELL_CODE,CELLNAME,CAMP_CODE From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER ='" + loginID.Substring(0,3) + "'";//
						_log.Debug("audienceSql:" + audienceSql);
						audienceCommand.CommandText = audienceSql;
						OracleDataReader audienceReader = audienceCommand.ExecuteReader();
						while (audienceReader.Read())
						{
							ListItem listItem = new ListItem();
							listItem.Text = audienceReader["CELLNAME"].ToString();
							listItem.Value = audienceReader["CELL_CODE"].ToString();
							listItem.Attributes.Add("data-campaigncode", audienceReader["CAMP_CODE"].ToString());
							_log.Debug("listItem.Text" + listItem.Text);
							MarketingAudienceList.Items.Add(listItem);
						}
					}
				}
			}

		}

		protected void search(object senderObject, EventArgs e)
		{
			string loginID = (string)Session["User"];
			int loginIDInt = 0;
			string cellcode = MarketingAudienceList.SelectedValue;
			string campaigncode = MarketingActtivityList.SelectedValue;
			if (int.TryParse(loginID.Substring(1), out loginIDInt))
			{
                //string mySetting = ConfigurationManager.AppSettings["MyKey"];

                //string connectionString = "User Id=<username>;Password=<password>;Data Source=<datasource>";

                string strConn = getCMAUTOConnString();// "server=.\\SQLExpress;database=databaseName;User ID=username;Password=password;Trusted_Connection=True;";
                _log.Debug("strConn: " + strConn);
                using (OracleConnection connection = new OracleConnection())
				{
					connection.ConnectionString = strConn;
					connection.Open();
					//Console.WriteLine("State: {0}", connection.State);
					//Console.WriteLine("ConnectionString: {0}", connection.ConnectionString);

					OracleCommand command = connection.CreateCommand();
					String sql = "Select TIMESTAMP, CMDM.ID_DEC(CUST_ID) as CUST_ID, CMDM.ACC_DEC(ACC_NO) as ACC_NO, CMDM.CARD_DEC(CARD_NO) as CARD_NO, RESV1, RESV2, RESV3, " +
						" RESV5, RESV6, RESV7, RESV8, RESV9, RESV10 From CMAUTO_AP.MKT_CAMP_SAMPLE " +
					  "Where CAMPAIGNOWNER = SUBSTRING('" + loginID + "', 1,3) And CAMP_CODE = '"
					  + campaigncode + "' And CELL_CODE = '" + cellcode + "' order by TIMESTAMP desc";// 
					_log.Debug("sql:" + sql);
					command.CommandText = sql;
					OracleDataReader reader = command.ExecuteReader();
					_log.Debug("reader.HasRows:" + reader.HasRows);
					while (reader.Read())
					{
						TableRow row = new TableRow();
						TableCell cell1 = new TableCell();
						string cell1string = reader["TIMESTAMP"].ToString();
						cell1string = cell1string.Substring(0, 4) + "/" + cell1string.Substring(4, 2) + "/" + cell1string.Substring(6, 2) + " " + cell1string.Substring(8, 2) + ":" + cell1string.Substring(10, 2) + ":" + cell1string.Substring(12, 2);
						cell1.Text = cell1string;
						TableCell cell2 = new TableCell();
						cell2.Text = reader["CUST_ID"].ToString();
						TableCell cell3 = new TableCell();
						cell3.Text = reader["ACC_NO"].ToString();
						TableCell cell4 = new TableCell();
						cell4.Text = reader["CARD_NO"].ToString();
						TableCell cell5 = new TableCell();
						cell5.Text = reader["RESV1"].ToString();
						TableCell cell6 = new TableCell();
						cell6.Text = reader["RESV2"].ToString();
						TableCell cell7 = new TableCell();
						cell7.Text = reader["RESV3"].ToString();
						TableCell cell8 = new TableCell();
						cell8.Text = "";
						TableCell cell9 = new TableCell();
						cell9.Text = reader["RESV5"].ToString();
						TableCell cell10 = new TableCell();
						cell10.Text = reader["RESV6"].ToString();
						TableCell cell11 = new TableCell();
						cell11.Text = reader["RESV7"].ToString();
						TableCell cell12 = new TableCell();
						cell12.Text = reader["RESV8"].ToString();
						TableCell cell13 = new TableCell();
						cell13.Text = reader["RESV9"].ToString();
						TableCell cell14 = new TableCell();
						cell14.Text = reader["RESV10"].ToString();


						row.Cells.Add(cell1);
						row.Cells.Add(cell2);
						row.Cells.Add(cell3);
						row.Cells.Add(cell4);
						row.Cells.Add(cell5);
						row.Cells.Add(cell6);
						row.Cells.Add(cell7);
						row.Cells.Add(cell8);
						row.Cells.Add(cell9);
						row.Cells.Add(cell10);
						row.Cells.Add(cell11);
						row.Cells.Add(cell12);
						row.Cells.Add(cell13);
						row.Cells.Add(cell14);
						ListSampleTable.Rows.Add(row);

					}
					OracleCommand audienceCommand = connection.CreateCommand();
					String audienceSql = "Select DISTINCT (CELL_CODE||CELLNAME),CELL_CODE,CELLNAME,CAMP_CODE From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER ='" + loginID.Substring(0,3) + "'";//
					_log.Debug("audienceSql:" + audienceSql);
					audienceCommand.CommandText = audienceSql;
					OracleDataReader audienceReader = audienceCommand.ExecuteReader();
					ListItem firstAudienceListItem = MarketingAudienceList.Items[0];
					MarketingAudienceList.Items.Clear();
					MarketingAudienceList.Items.Add(firstAudienceListItem);
					while (audienceReader.Read())
					{
						string thisCELLNAME= audienceReader["CELLNAME"].ToString();
						string thisCELLCODE = audienceReader["CELL_CODE"].ToString();
						string thisCAMPAIGNCODE = audienceReader["CAMP_CODE"].ToString();
						ListItem listItem = new ListItem();
						listItem.Text = thisCELLNAME;
						listItem.Value = thisCELLCODE;
						listItem.Attributes.Add("data-campaigncode", thisCAMPAIGNCODE);
						if (thisCELLCODE==cellcode && thisCAMPAIGNCODE==campaigncode) {
							listItem.Selected = true;
						}
						_log.Debug("listItem.Text: " + listItem.Text);
						MarketingAudienceList.Items.Add(listItem);
					}
				}
			}

		}
		protected string getCMAUTOConnString()
		{
            string filePath = "D:/test/settingCMDM_CMAUTO_AP.txt";
            string connString ="";

            try
            {
                // 讀取檔案的第一行
                 connString = File.ReadLines(filePath).First();

                // 印出第一行
                _log.Debug("getCMAUTOConnString: " + connString);
            }
            catch (FileNotFoundException)
            {
                _log.Error("找不到檔案: " + filePath);

            }
            catch (IOException e)
            {
                _log.Error("讀取檔案時發生錯誤: " + e.Message);
            }
            catch (Exception e)
            {
                _log.Error("發生未預期的錯誤: " + e.Message);
           
            }
            //string settingValue = ConfigurationManager.AppSettings["YourSettingKey"];
            //Console.WriteLine("Value of YourSettingKey: " + settingValue);
			//
			//
            //DBConfig cfg = new ConfigBO().selectDBConfigByVarName("CMDM_CMAUTO_AP");
			//string[] serverIp = cfg.cfgServer.VAR_VALUE.Split(':');
			//string connString = "Data Source=" + cfg.root.VAR_VALUE + ";Persist Security Info=True;User ID=" + cfg.cfgUser.VAR_VALUE + ";Password=" + cfg.cfgPwd.VAR_VALUE + ";";
			//Console.WriteLine(connString);
			return connString;
		}
	}
}
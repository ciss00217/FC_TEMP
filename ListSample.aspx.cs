using gbs.bao.etl.bo;
using gbs.bao.etl.entity;
using NLog;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
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
                        String activitySql = "Select DISTINCT (CAMP_CODE||CAMPAIGNNAME),CAMP_CODE,CAMPAIGNNAME From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER LIKE '00" + loginIDInt + "%'";

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
                        String audienceSql = "Select DISTINCT (CELL_CODE||CELLNAME),CELL_CODE,CELLNAME,CAMP_CODE From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER LIKE '00" + loginIDInt + "%'";
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
                    String sql = "Select TIMESTAMP, CMDM.ID_DEC(CUST_ID) as CUST_ID, CMDM.ACC_DEC(ACC_NO) as ACC_NO, CMDM.CARD_DEC(CARD_NO) as CARD_NO, RESV1, RESV2, RESV3, RESV4, RESV5, RESV6, RESV7, RESV8, RESV9, RESV10 From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER LIKE '00" + loginIDInt + "%' And CAMP_CODE = '" + campaigncode + "' And CELL_CODE = '" + cellcode + "' order by TIMESTAMP desc";

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
					String audienceSql = "Select DISTINCT (CELL_CODE||CELLNAME),CELL_CODE,CELLNAME,CAMP_CODE From CMAUTO_AP.MKT_CAMP_SAMPLE Where CAMPAIGNOWNER LIKE '00" + loginIDInt + "%'";//
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

        protected static string DecodeBase64(string encodedValue)
        {
            byte[] base64EncodedBytes = Convert.FromBase64String(encodedValue);
            return Encoding.UTF8.GetString(base64EncodedBytes);
        }

        protected static string EncodeBase64(string value)
        {
            byte[] plainTextBytes = Encoding.UTF8.GetBytes(value);
            return Convert.ToBase64String(plainTextBytes);
        }

        protected static Dictionary<string, string> ReadPropertiesFile(string filePath)
        {
            var properties = new Dictionary<string, string>();

            foreach (var line in File.ReadLines(filePath))
            {
                // 忽略空行和注釋行
                if (string.IsNullOrWhiteSpace(line) || line.StartsWith("#"))
                    continue;

                var delimiterIndex = line.IndexOf('=');
                if (delimiterIndex == -1)
                    continue;

                var key = line.Substring(0, delimiterIndex).Trim();
                var value = line.Substring(delimiterIndex + 1).Trim().Trim('"');

                properties[key] = value;
            }

            return properties;
        }
        protected string getCMAUTOConnString()
		{

            string connString = "";

            string filePath = "D:/IBM/IBMETL/ETLAdm/ETL_Manager.properties";
            Dictionary<string, string> properties = ReadPropertiesFile(filePath);

            string settingCMDM_CMAUTO_AP = "";
            properties.TryGetValue("settingCMDM_CMAUTO_AP", out settingCMDM_CMAUTO_AP);

            Console.WriteLine($"{DecodeBase64(settingCMDM_CMAUTO_AP)}");


            connString = DecodeBase64(settingCMDM_CMAUTO_AP);


            return connString;

		}
	}
}
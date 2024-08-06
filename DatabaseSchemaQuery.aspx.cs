using gbs.bao.etl.bo;
using gbs.bao.etl.entity;
using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Oracle.ManagedDataAccess.Client;
using System.IO;
using System.Text;

namespace ETLAdm.accessories
{
    public partial class DatabaseSchemaQuery : System.Web.UI.Page
    {

        protected void Page_Load(object senderObject, EventArgs e)
        {

            string strConn = getCMETLConnString();// "server=.\\SQLExpress;database=databaseName;User ID=username;Password=password;Trusted_Connection=True;";
            using (OracleConnection connection = new OracleConnection())
            {
                connection.ConnectionString = strConn;
                connection.Open();
                Console.WriteLine("State: {0}", connection.State);
                Console.WriteLine("ConnectionString: {0}",
                                  connection.ConnectionString);

                OracleCommand command = connection.CreateCommand();
                String sql = "SELECT T1.TABLE_NAME,T2.COMMENTS FROM ALL_TABLES T1,ALL_TAB_COMMENTS T2 WHERE T1.OWNER = 'CMDM' AND T1.TABLE_NAME = T2.TABLE_NAME ORDER BY T1.TABLE_NAME";
                command.CommandText = sql;

                OracleDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    TableRow row = new TableRow();
                    TableCell cell1 = new TableCell();
                    cell1.Text = reader["TABLE_NAME"].ToString();
                    TableCell cell2 = new TableCell();
                    cell2.Text = reader["COMMENTS"].ToString();
                    TableCell cell3 = new TableCell();
                    Button button = new Button();
                    button.Text = "查詢";
                    //button.Click += new EventHandler(this.tableQueryClick());
                    button.Click += (sender, EventArgs) => { tableQueryClick(sender, EventArgs, cell1.Text); };

                    //button.OnClientClick = "";
                    cell3.Controls.Add(button);

                    row.Cells.Add(cell1);
                    row.Cells.Add(cell2);
                    row.Cells.Add(cell3);
                    dbSchemaTable.Rows.Add(row);
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

        protected string getCMETLConnString()
        {
            string connString = "";

            string filePath = "D:/IBM/IBMETL/ETLAdm/ETL_Manager.properties";
            Dictionary<string, string> properties = ReadPropertiesFile(filePath);

            string settingCMETL = "";
            properties.TryGetValue("settingCMETL", out settingCMETL);

            Console.WriteLine($"{DecodeBase64(settingCMETL)}");


            connString = DecodeBase64(settingCMETL);

           




         //   DBConfig cfg = new ConfigBO().selectDBConfigByVarName("CMETL");
         //   string[] serverIp = cfg.cfgServer.VAR_VALUE.Split(':');
         //   string connString = "Data Source=" + cfg.root.VAR_VALUE + ";Persist Security Info=True;User ID=" + cfg.cfgUser.VAR_VALUE + ";Password=" + cfg.cfgPwd.VAR_VALUE + ";";
         //   Console.WriteLine(connString);
            // string connString = "Driver={" + cfg.cfgOdbcDriver.VAR_VALUE + "};data source=" + cfg.root.VAR_VALUE + ";user id=" + cfg.cfgUser.VAR_VALUE + ";password=" + cfg.cfgPwd.VAR_VALUE + ";";
            //tested case
            // "Driver={" + cfg.cfgOdbcDriver.VAR_VALUE + "};Server=" + cfg.root.VAR_VALUE +";UID=" + cfg.cfgUser.VAR_VALUE + ";PWD=" + cfg.cfgPwd.VAR_VALUE + ";";
            //"Driver={" + cfg.cfgOdbcDriver.VAR_VALUE + "};" + "Data Source=" + cfg.root.VAR_VALUE + ";User Id=" + cfg.cfgUser.VAR_VALUE + ";Password=" + cfg.cfgPwd.VAR_VALUE + ";Integrated Security = no; ";
            //"Server=" + cfg.cfgServer.VAR_VALUE + ":" + cfg.root.VAR_VALUE + "; UID = " + cfg.cfgUser.VAR_VALUE + "; PWD = " + cfg.cfgPwd.VAR_VALUE + "; ";
            //SqlConnection myConn = new SqlConnection("data source=140.119.19.78; initial catalog = test; user id = sa; password = 1qaz2wsx");
            return connString;
        }


        protected void dbQueryClick(object senderObject, EventArgs e)
        {
            ///*
            //    if (targetPath != null && targetPath.Text.Trim() != "")
            //    {
            //        //string strConn = "server=.\\SQLExpress;database=databaseName;User ID=username;Password=password;Trusted_Connection=True;";
            //        SqlConnection myConn = new SqlConnection("data source=140.119.19.78; initial catalog = test; user id = sa; password = 1qaz2wsx");

            //        //建立連接
            //        //SqlConnection myConn = new SqlConnection(strConn);


            //        //打開連接
            //        myConn.Open();
            //        String strSQL = @"select b,d from test where b like'%" + targetPath.Text.Trim() + "%'";


            //        //建立SQL命令對象
            //        SqlCommand myCommand = new SqlCommand(strSQL, myConn);


            //        //得到Data結果集
            //        SqlDataReader myDataReader = myCommand.ExecuteReader();
            //        //TableRow titleRow = dbSchemaTable.Rows[0];
            //        //dbSchemaTable
            //        for (int i = 1; i < dbSchemaTable.Rows.Count; i++)
            //        {
            //            dbSchemaTable.Rows.RemoveAt(i);
            //        }


            //        //讀取結果
            //        while (myDataReader.Read())
            //        {
            //            TableRow row = new TableRow();
            //            TableCell cell1 = new TableCell();
            //            cell1.Text = myDataReader["b"].ToString();
            //            TableCell cell2 = new TableCell();
            //            cell2.Text = myDataReader["d"].ToString();
            //            TableCell cell3 = new TableCell();
            //            Button button = new Button();
            //            button.Text = "查詢";
            //            //button.Click += new EventHandler(this.tableQueryClick());
            //            button.Click += (sender, EventArgs) => { tableQueryClick(sender, EventArgs, cell1.Text); };

            //            //button.OnClientClick = "";
            //            cell3.Controls.Add(button);

            //            row.Cells.Add(cell1);
            //            row.Cells.Add(cell2);
            //            row.Cells.Add(cell3);
            //            dbSchemaTable.Rows.Add(row);
            //        }
            //        myConn.Close();
            //        /*TableRow row = new TableRow();
            //        TableCell cell1 = new TableCell();
            //        cell1.Text = "dbQueryClickstart" + targetPath.Text + "strSQl"  ;
            //        row.Cells.Add(cell1);
            //        tableSchemaTable.Rows.Add(row);*/
            //    }
            //*/
        }

        protected void tableQueryClick(object senderObject, EventArgs e, string index)
        {

            string strConn = getCMETLConnString();// "server=.\\SQLExpress;database=databaseName;User ID=username;Password=password;Trusted_Connection=True;";
            using (OracleConnection connection = new OracleConnection())
            {
                connection.ConnectionString = strConn;
                connection.Open();
                Console.WriteLine("State: {0}", connection.State);
                Console.WriteLine("ConnectionString: {0}",
                                  connection.ConnectionString);

                OracleCommand command = connection.CreateCommand();
                string sql = @"SELECT T1.COLUMN_NAME,
CASE T1.DATA_TYPE
WHEN 'CHAR' THEN (T1.DATA_TYPE || '(' || T1.CHAR_LENGTH || ')')
WHEN 'VARCHAR2' THEN (T1.DATA_TYPE || '(' || T1.CHAR_LENGTH || ')')
WHEN 'NCHAR' THEN (T1.DATA_TYPE || '(' || T1.CHAR_LENGTH || ')')
WHEN 'NVARCHAR' THEN (T1.DATA_TYPE || '(' || T1.CHAR_LENGTH || ')')
WHEN 'NUMBER' THEN (T1.DATA_TYPE || '(' || T1.DATA_PRECISION || ',' || T1.DATA_SCALE || ')')
WHEN 'FLOAT' THEN (T1.DATA_TYPE || '(' || T1.DATA_PRECISION || ')')
ELSE T1.DATA_TYPE
END
""DATA_TYPE"",
T2.COMMENTS
FROM ALL_TAB_COLUMNS T1, ALL_COL_COMMENTS T2
WHERE T1.OWNER = 'CMDM'
AND T1.TABLE_NAME = '" + index + "' AND T2.TABLE_NAME = '" + index + "' AND T1.COLUMN_NAME = T2.COLUMN_NAME";
                command.CommandText = sql;

                OracleDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    TableRow row = new TableRow();
                    TableCell cell1 = new TableCell();
                    cell1.Text = reader["COLUMN_NAME"].ToString();
                    TableCell cell2 = new TableCell();
                    cell2.Text = reader["DATA_TYPE"].ToString();
                    TableCell cell3 = new TableCell();
                    cell3.Text = reader["COMMENTS"].ToString();


                    row.Cells.Add(cell1);
                    row.Cells.Add(cell2);
                    row.Cells.Add(cell3);
                    tableSchemaTable.Rows.Add(row);
                }
            }



        }

    }
}
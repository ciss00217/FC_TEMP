using System;
using System.Web;
using gbs.bao.etl.util;
using System.IO;
using System.Text;
using gbs.bao.etl.entity;
using gbs.bao.etl.bo;
using gbs.bao.etl.dao;
using Renci.SshNet;
using System.Net;
using System.Globalization;
using System.Web.UI;
using System.Linq;
using System.Collections.Generic;

namespace ETLAdm.accessories
{
    public partial class FTPUpload : EtlAdmPage
    {
        private int seq;
        private string uploadFileName;

        protected void Page_Load(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                ObjectDataSource_selectFtpServer.SelectParameters[0].DefaultValue = this.Project_Id + "";
                ObjectDataSource_selectFtpServer.SelectParameters[1].DefaultValue = UserName;

                //var today = DateTime.Today;
                //var thisMonth = new DateTime(today.Year, today.Month, 1);

                //dateStartTime.Text = thisMonth.AddMonths(-1).ToString("yyyy-MM-dd");
                //dateEndTime.Text = thisMonth.AddDays(-1).ToString("yyyy-MM-dd");
                dateStartTime.Text = DateTime.Now.ToString("yyyy-MM-dd");
                dateEndTime.Text = DateTime.Now.ToString("yyyy-MM-dd");
                fileTimeName.Text = getUploadFileName();
            }

            if (ViewState["uploadFileName"] != null)
            {
                uploadFileName = (string)ViewState["uploadFileName"];
            }

            if (ViewState["seq"] != null && Convert.ToInt32(ViewState["seq"]) != -1)
            {
                seq = Convert.ToInt32(ViewState["seq"]);
            }
            else
            {
                seq = -1;
            }

            string jscript = "function FileUploadPostBack(){" + ClientScript.GetPostBackEventReference(lbFileOnChange, "") + "};";
            Page.ClientScript.RegisterClientScriptBlock(this.GetType(), "Key", jscript, true);

            //btnOk.Attributes.Add("onclick", "if(typeof(Page_ClientValidate)=='function'){if(Page_ClientValidate('')==true){this.disabled = true;}}");
        }

        private void loadFTPUserInfo()
        {
            FTPUSD FTPUser = new FTPUserDAO().selectFTP(this.Project_Id, UserName, FtpServers.SelectedItem.Text);
            targetPath.Text = FTPUser.FTP_PATH;
            tbFTPUser.Text = String.IsNullOrEmpty(FTPUser.FTP_USR_NAME) ? "" : FTPUser.FTP_USR_NAME;
            if (String.IsNullOrEmpty(FTPUser.FTP_USR_NAME))
            {
                tbFTPPassword.Text = "";
                tbFTPPassword.Attributes.Add("value", "");
            }
            else
            {
                string decryptPassword = gbs.bao.etl.net.EtlEngineCommandTCPClient.Instance.DecryptString(FTPUser.FTP_PASSWORD).data as string;
                tbFTPPassword.Text = decryptPassword;
                tbFTPPassword.Attributes.Add("value", decryptPassword);
            }

            ftpProtocol.SelectedIndex = FTPUser.FTP_PROTOCOL.Equals('F') ? 0 : 1;
        }

        protected void ddOnPreRender(object sender, EventArgs e)
        {
            if (!IsPostBack)
            {
                if (FtpServers.SelectedItem != null)
                {
                    loadFTPUserInfo();
                }
            }
            else
            {
                tbFTPPassword.Attributes.Add("value", tbFTPPassword.Text);
            }
        }

        protected void lbFileOnChange_Click(object sender, EventArgs e)
        {
            if (inputFile.HasAttributes)
            {
                HttpPostedFile file = inputFile.PostedFile;
                DirectoryInfo targetDirectory = getTargetDirectory();
                uploadFileName = getUploadFileName();
                ViewState["uploadFileName"] = uploadFileName;

                if (SaveDataFile(file, targetDirectory).success)
                {
                    inputFile.Visible = false;
                    tbFileName.Text = inputFile.FileName;
                    tbFileName.Visible = true;
                    buttonReset.Visible = true;
                }
                else
                {
                    logger.Warn(string.Format("save tmp data file to server failed. file name={0}.", file.FileName));
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

      

            protected string getBdFtpAccountCodeString()
        {

         

                string connString = "";

                string filePath = "D:/IBM/IBMETL/ETLAdm/ETL_Manager.properties";
                Dictionary<string, string> properties = ReadPropertiesFile(filePath);

                string settingAccountCode = "";
                properties.TryGetValue("settingAccountCode", out settingAccountCode);

                Console.WriteLine($"{DecodeBase64(settingAccountCode)}");


                connString = DecodeBase64(settingAccountCode);


            return connString;
        }

        protected void upload_Click(object sender, EventArgs e)
        {
            checkSeq();
            FTPConfig FtpConfig = new ConfigBO().selectFTPConfig(this.Project_Id, FtpServers.SelectedItem.Text);
            string ftpServer = FtpConfig.root.VAR_VALUE;
            string userName = tbFTPUser.Text;
            string password = getBdFtpAccountCodeString();

            //check file name
            DirectoryInfo targetDirectory = getTargetDirectory();
            string tmpFileName = userInputName.Text + "_" + fileTimeName.Text;
            string oldPath = targetDirectory.FullName + @"\" + uploadFileName;
            string newPath = targetDirectory.FullName + @"\" + tmpFileName;

            try
            {
                if (File.Exists(newPath + ".D"))
                {
                    File.Delete(newPath + ".D");
                }

                File.Move(oldPath + ".D", newPath + ".D");
                uploadFileName = tmpFileName;
                ViewState["uploadFileName"] = uploadFileName;

                FileInfo dataFileInfo = new FileInfo(newPath + ".D");
                long dataFileSize = dataFileInfo.Length;

                int dataFileRows = 0;
                using (StreamReader fileStream = new StreamReader(newPath + ".D"))
                {
                    dataFileRows = 0;
                    while (fileStream.ReadLine() != null)
                    {
                        dataFileRows++;
                    }
                    fileStream.Close();
                }

                FileInfo handleFileInfo = null;
                if (cbUploadHandleFile.Checked)
                {
                    SaveHandleFile(targetDirectory, dataFileRows);
                    handleFileInfo = new FileInfo(newPath + ".H");
                }

                DateTime startTime = DateTime.Now;
                bool bUploadResult = false;

                try
                {
                    //FTP test Server URL. ftp://ftp.swfwmd.state.fl.us/
                    //ftpServer = "ftp.swfwmd.state.fl.us";
                    //ftpServer = "10.10.23.171";
                    //FTP Folder name. Leave blank if you want to upload to root folder.
                    //ftpFolder = "pub/incoming/";
                    //targetPath.Text = "pub/incoming";
                    //FTP account
                    //"Anonymous",email
                    //userName = "bdftp";
                    //password = "2019pw@q1!!!";
                    //userName = "chika";
                    //password = "31415926";

                    //SFTP test server
                    //ftpServer = "test.rebex.net";
                    //userName = "demo";
                    //password = "password";

                    if (ftpProtocol.SelectedValue == "F")
                    {
                        string ftpPath = "ftp://" + ftpServer + "/";
                        if (!String.IsNullOrEmpty(targetPath.Text))
                        {
                            ftpPath += targetPath.Text + "/";
                        }

                        byte[] dataFileBytes = null;

                        using (StreamReader dataStreamReader = new StreamReader(newPath + ".D"))
                        {
                            dataFileBytes = Encoding.UTF8.GetBytes(dataStreamReader.ReadToEnd());
                            dataStreamReader.Close();
                        }

                        //Create FTP Request.
                        FtpWebRequest dataRequest = (FtpWebRequest)WebRequest.Create(ftpPath + uploadFileName + tbDataFileExtension.Text);
                        dataRequest.Method = WebRequestMethods.Ftp.UploadFile;

                        //Enter FTP Server credentials.
                        dataRequest.Credentials = new NetworkCredential(userName, password);
                        dataRequest.ContentLength = dataFileInfo.Length;
                        dataRequest.UsePassive = true;
                        dataRequest.UseBinary = true;
                        dataRequest.EnableSsl = false;

                        using (Stream requestStream = dataRequest.GetRequestStream())
                        {
                            requestStream.Write(dataFileBytes, 0, dataFileBytes.Length);
                            requestStream.Close();
                        }

                        FtpWebResponse dataResponse = (FtpWebResponse)dataRequest.GetResponse();
                        if ((Int32)dataResponse.StatusCode < 300)
                        {
                            bUploadResult = true;
                        }
                        else
                        {
                            logger.Warn(string.Format("upload data file fail, status code={0}", dataResponse.StatusCode));
                        }

                        dataResponse.Close();

                        if (cbUploadHandleFile.Checked)
                        {
                            byte[] handleFileBytes = null;

                            using (StreamReader handleStreamReader = new StreamReader(newPath + ".H"))
                            {
                                handleFileBytes = Encoding.UTF8.GetBytes(handleStreamReader.ReadToEnd());
                                handleStreamReader.Close();
                            }

                            //Create FTP Request.
                            FtpWebRequest handleRequest = (FtpWebRequest)WebRequest.Create(ftpPath + uploadFileName + tbHandleFileExtension.Text);
                            handleRequest.Method = WebRequestMethods.Ftp.UploadFile;

                            //Enter FTP Server credentials.
                            handleRequest.Credentials = new NetworkCredential(userName, password);
                            handleRequest.ContentLength = handleFileInfo.Length;
                            handleRequest.UsePassive = true;
                            handleRequest.UseBinary = true;
                            handleRequest.EnableSsl = false;

                            using (Stream requestStream = handleRequest.GetRequestStream())
                            {
                                requestStream.Write(handleFileBytes, 0, handleFileBytes.Length);
                                requestStream.Close();
                            }

                            FtpWebResponse handleResponse = (FtpWebResponse)handleRequest.GetResponse();

                            if ((Int32)handleResponse.StatusCode >= 300)
                            {
                                bUploadResult = false;
                                logger.Warn(string.Format("upload handle file fail, status code={0}", dataResponse.StatusCode));
                            }

                            handleResponse.Close();
                        }
                    }
                    else //SFTP
                    {
                        using (var client = new SftpClient(ftpServer, userName, password))
                        {
                            if (!client.IsConnected)
                            {
                                client.Connect();
                            }

                            using (var dataFileStream = dataFileInfo.Open(FileMode.Open))
                            {
                                client.BufferSize = 4 * 1024; // bypass payload error large files
                                client.UploadFile(dataFileStream, targetPath.Text + @"\" + uploadFileName + tbDataFileExtension.Text);
                                //TODO this is async call, need to check the result and disconnect
                            }

                            if (cbUploadHandleFile.Checked)
                            {
                                using (var handleFileStream = handleFileInfo.Open(FileMode.Open))
                                {
                                    client.BufferSize = 4 * 1024; // bypass payload error large files
                                    client.UploadFile(handleFileStream, targetPath.Text + @"\" + uploadFileName + tbHandleFileExtension.Text);
                                    //TODO this is async call, need to check the result and disconnect
                                }
                            }

                            bUploadResult = true;
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.Warn(string.Format("exception when uploading file, exception={0}", ex.Message));
                    lErrorMessage.Text = "(" + ex.Message + ")";
                }

                DateTime endTime = DateTime.Now;

                FTPUserBO bo = new FTPUserBO();
                bo.UserName = this.UserName;
                string encryptPassword = gbs.bao.etl.net.EtlEngineCommandTCPClient.Instance.EncryptString(password).data as string;
                //string encryptPassword = tbFTPPassword.Text;

                bo.updateFTPUser(this.Project_Id, FtpServers.SelectedItem.Text, targetPath.Text, tbFTPUser.Text, encryptPassword, System.Convert.ToChar(ftpProtocol.SelectedValue[0]));

                ResultBean ftpLogAddResult = setMessage(bo.addFTPLog(new FTPLOG
                {
                    PRJ_ID = this.Project_Id,
                    SEQ = seq,
                    FILE_NAME = uploadFileName,
                    FILE_CNT = dataFileRows,
                    FTP_START_TIM = startTime,
                    FTP_END_TIM = endTime,
                    FTP_STATUS = bUploadResult ? 'S' : 'F',
                    TBL_UPDATER = this.UserName,
                    TBL_UPD_TIM = DateTime.Now
                }));

                lUploadResult.Text = bUploadResult ? "成功" : "失敗";
                if (bUploadResult)
                {
                    lErrorMessage.Text = "";
                }

                tbFileNameLine1.Text = uploadFileName + tbDataFileExtension.Text;
                tbFileSizeLine1.Text = "" + dataFileSize;
                tbFileTimeLine1.Text = endTime.ToString();
                tbFileRowsLine1.Text = "" + dataFileRows;

                if (cbUploadHandleFile.Checked)
                {
                    tbFileNameLine2.Text = uploadFileName + tbHandleFileExtension.Text;
                    tbFileSizeLine2.Text = "" + handleFileInfo.Length;
                    tbFileTimeLine2.Text = endTime.ToString();
                    tbFileRowsLine2.Text = "1";
                }
                else
                {
                    tbFileNameLine2.Text = "";
                    tbFileSizeLine2.Text = "";
                    tbFileTimeLine2.Text = "";
                    tbFileRowsLine2.Text = "";
                }

                seq = -1;
                ViewState["seq"] = -1;
                fileReset();
                btnOk.Enabled = true;
                resultPanel.Visible = true;
            }
            catch (Exception ex)
            {
                logger.Warn(string.Format("file operation exception when preparing ftp upload:{0}", ex.Message));
            }
        }

        protected void selectedServerChanged(object sender, EventArgs e)
        {
            loadFTPUserInfo();
        }

        protected void fileNameSettingChanged(object sender, EventArgs e)
        {
            fileTimeName.Text = getUploadFileName();
        }

        private DirectoryInfo getTargetDirectory()
        {
            FileInfo fi = new FileInfo(Server.MapPath("~/Default.aspx"));
            DirectoryInfo dir = null;
            DirectoryInfo[] dirs = fi.Directory.GetDirectories("App_Data");
            if (dirs != null)
            {
                dir = dirs[0];
            }
            else
            {
                dirs = fi.Directory.Parent.GetDirectories("ETLTemp");
                if (dirs != null)
                {
                    dir = dirs[0];
                }
            }

            return dir;
        }

        private string getUploadFileName()
        {
            string fileTime = null;
            DateTime targetDay;

            CultureInfo provider = CultureInfo.InvariantCulture;
            try
            {
                DateTime startDay = DateTime.ParseExact(dateStartTime.Text, "yyyy-MM-dd", provider);
                DateTime endDay = DateTime.ParseExact(dateEndTime.Text, "yyyy-MM-dd", provider);

                switch (fileNameSetting.SelectedValue)
                {
                    case "year":
                        targetDay = new DateTime(DateTime.Parse(dateStartTime.Text).Year, 1, 1);
                        fileTime = targetDay.ToString("yyyyMMdd");
                        break;
                    case "month":
                        targetDay = startDay.AddDays(1 - startDay.Day);
                        fileTime = targetDay.ToString("yyyyMMdd");
                        break;
                    case "week":
                        targetDay = startDay;
                        fileTime = targetDay.ToString("yyyyMMdd");
                        break;
                    case "day":
                        targetDay = endDay;
                        fileTime = targetDay.ToString("yyyyMMdd");
                        break;
                    case "timestamp":
                        fileTime = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                        break;
                    case "seq":
                        checkSeq();
                        string seqString = "" + seq;
                        fileTime = DateTime.Now.ToString("yyyyMMdd_") + seqString.PadLeft(3, '0');
                        break;
                    default:
                        fileTime = DateTime.Now.ToString("yyyyMMdd");
                        break;
                }
            }
            catch (Exception)
            {
                string script = "alert(\"請輸入正確的日期資料格式(yyyy-mm-dd)!\");";
                ScriptManager.RegisterStartupScript(this, GetType(), "ServerControlScript", script, true);
            }

            return fileTime;
        }

        private ResultBean SaveDataFile(HttpPostedFile file, DirectoryInfo dir)
        {
            if (dir == null)
            {
                return new ResultBean() { success = false, message = "無法建立暫存資料夾，請洽系統管理人員" };
            }
            else
            {
                string path = dir.FullName + @"\" + uploadFileName + ".D";
                file.SaveAs(path);
                return new ResultBean() { success = true, data = path };
            }
        }

        private ResultBean SaveHandleFile(DirectoryInfo dir, int dataFileRows)
        {
            if (dir == null)
            {
                return new ResultBean() { success = false, message = "無法建立暫存資料夾，請洽系統管理人員" };
            }
            else
            {
                string path = dir.FullName + @"\" + uploadFileName + ".H";

                if (File.Exists(path))
                {
                    File.Delete(path);
                }

                StreamWriter w;
                w = File.CreateText(path);
                string outputText = DateTime.Now.ToString("yyyy-MM-dd") + "," + DateTime.Now.ToString("yyyy-MM-dd") + "," 
                    + uploadFileName + tbDataFileExtension.Text + "," + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "," + dataFileRows + "," + seq;

                w.WriteLine(outputText);
                w.Flush();
                w.Close();

                return new ResultBean() { success = true, data = path };
            }
        }

        private void checkSeq()
        {
            if (seq == -1)
            {
                seq = new SeqBO().nextIntVal(typeof(FTPLOG));
                ViewState["seq"] = seq;
            }
        }

        protected void fileReset(object sender, EventArgs e)
        {
            fileReset();
        }

        private void fileReset()
        {
            tbFileName.Text = "";
            tbFileName.Visible = false;
            buttonReset.Visible = false;
            inputFile.Visible = true;
        }

        protected void restoreFileExtionsion_Click(object sender, EventArgs e)
        {
            tbDataFileExtension.Text = ".D";
            tbHandleFileExtension.Text = ".H";
        }

        protected void inputNameExtionsion1_Click(object sender, EventArgs e)
        {
            userInputName.Text = "UPL_CAMP_LEADS";
        }

        protected void inputNameExtionsion2_Click(object sender, EventArgs e)
        {
            userInputName.Text = "UPL_EXCL_LEADS";
        }

        protected void inputNameExtionsion3_Click(object sender, EventArgs e)
        {
            userInputName.Text = "UPL_CAMP_LEADS_EFL";
        }

    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using gbs.bao.etl.util;
using gbs.bao.etl.dao;
using gbs.bao.etl.entity;
using gbs.bao.etl.bo.json;
using gbs.bao.etl.bo.json.entity;
using System.Web.Script.Serialization;
using gbs.bao.etl.bo.json.core;
using gbs.bao.etl.entity.proxy;
using System.Collections;
using System.Text;
using System.IO;

namespace ETLAdm.accessories
{
    /// <summary>
    /// CommandName - Control by show detail button
    ///     ''  : initial (Job flow dashboard)
    ///     '0' : Job flow dependence.  
    ///     '1' : Job dependence in the job flow.
    /// 
    /// CommandArgument - Control by browser to execute command and response from server will modify again for show.
    /// 
    /// </summary>
    public partial class FTPDashboard : EtlAdmPage
    {
        private string _TheSort;

        public string TheSort
        {
            get
            {
                _TheSort = ViewState["TheSort"] as string;
                return _TheSort;
            }
            set
            {
                _TheSort = value;
                ViewState["TheSort"] = _TheSort;
            }
        }

        public HiddenField TheCommandArgument { get { return hdnCommandArgument; } }

        public DropDownList TheAutoRefreshMin { get { return AutoRefreshMin; } }

        public DropDownList TheAutoRefreshSec { get { return AutoRefreshSec; } }

        public CheckBox TheCbxAutoRefresh { get { return cbxAutoRefresh; } }

        public bool IsETLEXE
        {
            get
            {
                return this.User.IsInRole("ETLEXE");
            }
        }

        protected void Page_Load(object sender, EventArgs e)
        {
            CurrentDateTime.Text = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss");

            if (!IsPostBack)
            {
                if (PreviousPage == null)
                {
                    bindFromQueryString();
                    this.hdnCommandName.Value = string.Empty;
                    this.hdnCommandArgument.Value = string.Empty;
                    GridView_DataBind();
                }
                else
                {
                    this.hdnCommandName.Value = "1";
                    this.hdnCommandArgument.Value = Request.Form["hdnOriginalFlowID"];
                    bindFromQueryString();                    
                }                                

            }

            this.hdnJobFlowSt.Value = "";
            if (this.cbStatusS.Checked == true) this.hdnJobFlowSt.Value += "S";
            if (this.cbStatusF.Checked == true) this.hdnJobFlowSt.Value += "F";
            if (this.cbStatusP.Checked == true) this.hdnJobFlowSt.Value += "P";

            string href = string.Concat(
                    "~/monitor/JobStepDashboard.aspx",
                    "&JobFlowSt=", this.hdnJobFlowSt.Value,//this.ddlJobFlowSt.SelectedValue,
                    "&BeginDT=" + Server.UrlEncode(this.tbxBeginDT.Text),
                    "&EndDT=" + Server.UrlEncode(this.tbxEndDT.Text)
                );
            this.btnSubmitToJobStep.PostBackUrl =  href ;                    
        }

        private void bindFromQueryString()
        {
            string jobFlowSt = Request.QueryString["JobFlowSt"];
            string beginDt = Server.UrlDecode(Request.QueryString["BeginDT"]);
            string endDt = Server.UrlDecode(Request.QueryString["EndDT"]);
            string freq = Server.UrlDecode(Request.QueryString["Freq"]);
            string freqName = Server.UrlDecode(Request.QueryString["FreqName"]);

            if (!string.IsNullOrEmpty(jobFlowSt))
            {
                this.cbStatusS.Checked = jobFlowSt.Contains("S");
                this.cbStatusF.Checked = jobFlowSt.Contains("F");
                this.cbStatusP.Checked = jobFlowSt.Contains("P");
            }
            this.tbxBeginDT.Text = beginDt;
            this.tbxEndDT.Text = endDt;
        }        
        
        protected void GridView_DataBind()
        {
            FTPLogDAO dao = new FTPLogDAO();
            var st = dao.selectAllLogs(this.Project_Id)
                .Where(x =>
                    (    (this.cbStatusS.Checked && x.e1.FTP_STATUS.ToString().Equals("S")) || 
                            (this.cbStatusF.Checked && x.e1.FTP_STATUS.ToString().Equals("F")) || 
                            (this.cbStatusP.Checked && x.e1.FTP_STATUS.ToString().Equals("P")))
                    && (string.IsNullOrEmpty(this.tbxBeginDT.Text) || (x.e1 == null || x.e1.FTP_START_TIM == null ? false : x.e1.FTP_START_TIM.Value.CompareTo(DateTime.Parse(this.tbxBeginDT.Text)) >= 0))
                    && (string.IsNullOrEmpty(this.tbxEndDT.Text) || (x.e1 == null || x.e1.FTP_END_TIM == null ? false : x.e1.FTP_END_TIM.Value.CompareTo(DateTime.Parse(this.tbxEndDT.Text)) <= 0))
                    && (string.IsNullOrEmpty(this.tbxMatchString.Text) || (x.e1 == null || x.e1.FILE_NAME == null ? false : x.e1.FILE_NAME.StartsWith(tbxMatchString.Text.Trim(), true, null)))
                    ); // datasource

            Control control = lblTitle_D.NamingContainer;
            GridView view = control.FindControl("GridView_Type_D") as GridView;

            string sortExpression = "";
            string sortDirection = "";

            if (!string.IsNullOrEmpty(TheSort))
            {
                string[] t = TheSort.Split("&".ToCharArray());
                sortExpression = t[1];
                sortDirection = t[2];
            }
            else
            {
                sortExpression = "FTP_START_TIM";
                sortDirection = SortDirection.Descending.ToString();
            }

            var v = from _v in st
                    select new
                    {
                        FILE_NAME = _v.e1 == null ? null : _v.e1.FILE_NAME,
                        FTP_START_TIM = _v.e1 == null ? null : _v.e1.FTP_START_TIM,
                        FTP_END_TIM = _v.e1 == null ? null : _v.e1.FTP_END_TIM,
                        FTP_STATUS = _v.e1 == null ? null : _v.e1.FTP_STATUS,
                        FILE_CNT = _v.e1 == null ? null : _v.e1.FILE_CNT,
                        JOB_START_DT = _v.e2 == null ? null : _v.e2.JOB_START_DT,
                        JOB_END_DT = _v.e2 == null ? null : _v.e2.JOB_END_DT,
                        RUN_START_TIM = _v.e2 == null ? null : _v.e2.RUN_START_TIM,
                        RUN_END_TIM = _v.e2 == null ? null : _v.e2.RUN_END_TIM,
                        JOB_STATUS = _v.e2 == null ? 'W' : Char.Parse(_v.e2.JOB_STATUS)
                    };

            if (SortDirection.Ascending.ToString().Equals(sortDirection))
            {
                if ("FILE_NAME".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.FILE_NAME);
                }
                else if ("FTP_START_TIM".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.FTP_START_TIM);
                }
                else if ("FTP_END_TIM".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.FTP_END_TIM);
                }
                else if ("FTP_STATUS".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.FTP_STATUS);
                }
                else if ("FILE_CNT".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.FILE_CNT);
                }
                else if ("JOB_START_DT".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.JOB_START_DT);
                }
                else if ("JOB_END_DT".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.JOB_END_DT);
                }
                else if ("RUN_START_TIM".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.RUN_START_TIM);
                }
                else if ("RUN_END_TIM".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.RUN_END_TIM);
                }
                else if ("JOB_STATUS".Equals(sortExpression))
                {
                    v = v.OrderBy(x => x.JOB_STATUS);
                }
            }
            else
            {
                if ("FILE_NAME".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.FILE_NAME);
                }
                else if ("FTP_START_TIM".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.FTP_START_TIM);
                }
                else if ("FTP_END_TIM".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.FTP_END_TIM);
                }
                else if ("FTP_STATUS".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.FTP_STATUS);
                }
                else if ("FILE_CNT".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.FILE_CNT);
                }
                else if ("JOB_START_DT".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.JOB_START_DT);
                }
                else if ("JOB_END_DT".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.JOB_END_DT);
                }
                else if ("RUN_START_TIM".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.RUN_START_TIM);
                }
                else if ("RUN_END_TIM".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.RUN_END_TIM);
                }
                else if ("JOB_STATUS".Equals(sortExpression))
                {
                    v = v.OrderByDescending(x => x.JOB_STATUS);
                }
            }

            if (view == null)
            {
                logger.Warn("Can't found GridView name - " + "GridView_Type_D");
            }
            else
            {
                view.DataSource = v;
                view.DataBind();
            }
        }
        protected void btnReflash_Click(object sender, EventArgs args)
        {
            GridView_DataBind();
        }

        protected void btnSubmit_Click(object sender, EventArgs args)
        {
            //string commandName = this.hdnCommandName.Value;
            //string commandArgument = this.hdnCommandArgument.Value;
        }

        protected void btnExecutionSubmit_Click(object sender, EventArgs args)
        {
            if (IsReFlesh)
            {
                switch (hdnCommandName.Value)
                {
                    case "":
                        GridView_DataBind();
                        break;
                    default:
                        hdnCommandArgument.Value = Request.Form["sel_job_flow"]; // reset argument to job flow id for select option rebind data.
                        break;
                }                
                return;
            }
            RunJobBean entity = new JavaScriptSerializer().Deserialize<RunJobBean>(hdnCommandArgument.Value);

            switch (hdnCommandName.Value)
            {
                case "":
                    break;
                case "0":                    
                    break;
                case "1":                    
                    break;
                default:
//                    setMessage(false, "Unknow command name: " + hdnCommandName.Value);
                    Response.Redirect("Dashboard.aspx");
                    break;
            }
            if (entity != null)
            {
                ISignatureBo bo = JsonSvcBoFactory.Instance.CreateSignatureBo(JsonSvcConst.RunJob);
                bo.UserName = this.UserName;
                ResultBean bean = setMessage(((ITypeBo<RunJobBean>)bo).Execute(entity));
                if (hdnCommandName.Value == string.Empty)
                {
                    GridView_DataBind(); 
                }
                RegistSessionKeyUpdate();
            }
            if (Request.Form["sel_job_flow"] != null)
            {
                hdnCommandArgument.Value = Request.Form["sel_job_flow"]; // reset argument to job flow id for select option rebind data.
            }
        }

        protected void btnCancelSubmit_Click(object sender, EventArgs args)
        {
            if (IsReFlesh)
            {
                switch (hdnCommandName.Value) { 
                    case "":
                    case "0":
                        GridView_DataBind();
                        break;
                    default:
                        hdnCommandArgument.Value = Request.Form["sel_job_flow"]; // reset argument to job flow id for select option rebind data.
                        break;
                }
                
                return;
            }
            JobDefineBean entity = null;
            int ap_id = Int32.Parse(hdnCommandArgument.Value);
            
            switch (hdnCommandName.Value)
            {
                case "":
                case "0":
                    entity = new JobDefineBean()
                    {
                        PRJ_ID = this.Project_Id,
                        JOB_FLOW_ID = ap_id
                    };
                    break;
                case "1":
                    entity = new JobDefineBean()
                    {
                        PRJ_ID = this.Project_Id,
                        AP_ID = ap_id
                    };
                    hdnCommandArgument.Value = Request.Form["sel_job_flow"]; // reset argument to job flow id for select option rebind data.
                    break;
                default:
//                    setMessage(false, "Unknow command name: " + hdnCommandName.Value);
                    Response.Redirect("Dashboard.aspx");
                    break;
            }
            if (entity != null)
            {
                ISignatureBo bo = JsonSvcBoFactory.Instance.CreateSignatureBo(JsonSvcConst.CancelJob);
                bo.UserName = this.UserName;
                ResultBean bean = setMessage(((ITypeBo<JobDefineBean>)bo).Execute(entity));
                if (hdnCommandName.Value == string.Empty)
                {
                    GridView_DataBind();
                }
                RegistSessionKeyUpdate();
            }
        }

        protected void GridView_Type_Sorting(object sender, GridViewSortEventArgs e)
        {
            GridView view = sender as GridView;
            if (string.IsNullOrEmpty(TheSort))
            {
                TheSort = view.ID + "&" + e.SortExpression + "&" + e.SortDirection;
            }
            else
            {
                string[] theSort = TheSort.Split("&".ToCharArray());
                if (view.ID.Equals(theSort[0]))
                {
                    if (e.SortExpression.Equals(theSort[1]))
                    {
                        if (SortDirection.Ascending.ToString().Equals(theSort[2]))
                        {
                            TheSort = view.ID + "&" + e.SortExpression + "&" + SortDirection.Descending;
                        }
                        else
                        {
                            TheSort = view.ID + "&" + e.SortExpression + "&" + SortDirection.Ascending;
                        }
                    }
                    else
                    {
                        TheSort = view.ID + "&" + e.SortExpression + "&" + e.SortDirection;
                    }
                }
                else
                {
                    TheSort = view.ID + "&" + e.SortExpression + "&" + e.SortDirection;
                }
            }
            GridView_DataBind();
        }

        protected void refresh(object sender, EventArgs e)
        {
            GridView_DataBind();
        }

        protected void cbStatus_CheckedChanged(object sender, EventArgs e)
        {
            GridView_DataBind();          
        }

        protected void btnSelectAll_Click(object sender, EventArgs e)
        {
            cbStatusS.Checked = true;
            cbStatusF.Checked = true;
            cbStatusP.Checked = true;

            GridView_DataBind();          
        }

        protected void btnCancelAll_Click(object sender, EventArgs e)
        {
            cbStatusS.Checked = false;
            cbStatusF.Checked = false;
            cbStatusP.Checked = false;

            GridView_DataBind();          
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
    }
}

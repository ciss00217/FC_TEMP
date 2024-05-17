package com.ibm.dpft.engine.core.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.ibm.dpft.engine.core.common.GlobalConstants;

class StreamConsumer extends Thread {
    InputStream is;
    String type;
    String filename;
     
    StreamConsumer (InputStream is, String type, String filename) {
        this.is = is;
        this.type = type;
        this.filename = filename;
    }
     
    public void run () {
        try {
        	File f = new File(filename);
        	if(!f.getParentFile().exists())
        		f.getParentFile().mkdirs();
        	
        	Writer out = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(f), "utf-8"));
            InputStreamReader isr = new InputStreamReader (is);
            BufferedReader br = new BufferedReader (isr);
            String line = null;
            while ((line = br.readLine()) != null){
            	out.write(line + GlobalConstants.FILE_EOL);
            }
            out.flush();
            out.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();  
            DPFTLogger.error(this, "Error :", ioe);
        }
    }
}

public class DPFTBashRunner {
	private String cmd = null;
	private String logfile = null;

	public void setBashCmd(String cmd) {
		
		this.cmd = cmd;
	}
	
	public String getBashCmd() {
		return cmd;
	}

	public int execute() throws IOException, InterruptedException {
		return execute(true);
	}

	public void setLogFile(String logfile) {
		
		this.logfile = logfile;
	}

	public int execute(boolean wait_rtn_code) throws IOException, InterruptedException {
		DPFTLogger.debug(this, "Executing Bash Command = " + cmd);
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec(cmd);
		StreamConsumer errConsumer = new StreamConsumer(p.getErrorStream(), "Error", logfile + ".err.log");
		StreamConsumer outConsumer = new StreamConsumer(p.getInputStream(), "Output", logfile + ".out.log");
		errConsumer.start();
		outConsumer.start();
		if(!wait_rtn_code)
			return 0;
		int extCode = p.waitFor();
		DPFTLogger.info(this, "Bash Execute Exit Code :" + extCode);
		return extCode;
	}

}

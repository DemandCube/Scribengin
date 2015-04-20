package com.neverwinterdp.scribengin.dataflow.test;

import java.util.List;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.util.text.TabularFormater;

abstract public class DataflowCommandTest {
  public void run(ScribenginShell shell) {
    try { 
      doRun(shell) ;
    } catch(Exception ex) {
      ex.printStackTrace(); 
    }
  }
  
  abstract public void doRun(ScribenginShell shell) throws Exception ;
  
  protected void report(ScribenginShell shell, List<ExecuteLog> executeLogs) throws Exception {
    TabularFormater formater = new TabularFormater("Description", "Success", "Duration") ;
    for(ExecuteLog sel : executeLogs) {
      Object[] cells = {
        sel.getDescription(), sel.isSuccess(), sel.getStop() - sel.getStart()  
      };
      formater.addRow(cells);
    }
    shell.console().println(formater.getFormatText());
  }
  
  protected void junitReport(String junitReportFile, List<ExecuteLog> executeLogs) throws Exception {
    System.err.println("");
    System.err.println("!!!!!!!!!!!!!!");
    for(ExecuteLog log : executeLogs){
      System.err.println(log.getFormatText());
    }
    System.err.println("??????????????");
    System.err.println("");
  }
}

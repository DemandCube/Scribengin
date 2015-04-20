package com.neverwinterdp.scribengin.dataflow.test;

import java.util.List;

import com.neverwinterdp.scribengin.client.shell.ScribenginShell;

abstract public class DataflowCommandTest {
  public void run(ScribenginShell shell) {
    try { 
      doRun(shell) ;
    } catch(Exception ex) {
      ex.printStackTrace(); 
    }
  }
  
  abstract public void doRun(ScribenginShell shell) throws Exception ;
  
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

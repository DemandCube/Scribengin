package com.neverwinterdp.scribengin.dataflow.test;

import java.io.File;
import java.util.List;

import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

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
  
  //TODO: 1. Figure out what to assert on
  protected void junitReport(String junitReportFile, List<ExecuteLog> executeLogs) throws Exception {
    TestSet testSet = new TestSet();
    int testNum = 0;
    
    for(ExecuteLog log : executeLogs){
      testSet.addTestResult(newTestResult(++testNum,
          log.getDescription(),
          log.getStop() - log.getStart() < 120000));
    }
    
    File file = new File(junitReportFile);
    try{
      file.getParentFile().mkdirs();
    } catch( NullPointerException e){}
    file.createNewFile();
    TapProducer tapProducer = TapProducerFactory.makeTapJunitProducer(getClass().getSimpleName());
    tapProducer.dump(testSet, file);
  }
  
  private TestResult newTestResult(int testNum, String desc, boolean passed) {
    TestResult tr = null;
    if (passed) {
      tr = new TestResult(StatusValues.OK, testNum);
    } else {
      tr = new TestResult(StatusValues.NOT_OK, testNum);
    }
    tr.setDescription(desc);
    return tr;
  }
}

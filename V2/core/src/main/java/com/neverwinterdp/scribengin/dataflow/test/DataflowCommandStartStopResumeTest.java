package com.neverwinterdp.scribengin.dataflow.test;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.event.WaitingOrderNodeEventListener;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.event.DataflowEvent;
import com.neverwinterdp.util.text.TabularFormater;

public class DataflowCommandStartStopResumeTest extends DataflowCommandTest {
  final static public String TEST_NAME = "start-stop-resume";

  @Parameter(names = "--dataflow-name", description = "The command should repeat in this failurePeriod of time")
  String dataflowName = "kafka-to-kafka";
  
  @Parameter(names = "--wait-before-start", description = "The command should repeat in this failurePeriod of time")
  long waitBeforeStart = -1;
  
  @Parameter(names = "--sleep-before-execute", description = "The command should repeat in this failurePeriod of time")
  long sleepBeforeExecute = 10000;
  
  @Parameter(names = "--max-wait-for-stop", description = "The command should repeat in this failurePeriod of time")
  long maxWaitForStop = 20000;
  
  @Parameter(names = "--max-wait-for-resume", description = "The command should repeat in this failurePeriod of time")
  long maxWaitForResume = 20000;

  @Parameter(names = "--print-summary", description = "Enable to dump the registry at the end")
  protected boolean printSummary = false;
  
  //TODO: implement junit report for this. 
  @Parameter(names = "--junit-report", description = "Enable to dump the registry at the end")
  String  junitReportFile = null;
  
  public void doRun(ScribenginShell shell) throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient() ;
    DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowName);
    DataflowLifecycleStatus dataflowStatus = dflClient.getStatus();
    int stopCount = 0; 
    int stopCompleteCount = 0; 
    int resumeCount = 0 ;
    int resumeCompleteCount = 0; 
    List<ExecuteLog> executeLogs = new ArrayList<ExecuteLog>() ;
    try {
      if(waitBeforeStart > 0) {
        Thread.sleep(waitBeforeStart);
      }
      while(dataflowStatus != DataflowLifecycleStatus.FINISH && dataflowStatus != DataflowLifecycleStatus.TERMINATED) {
        if(sleepBeforeExecute > 0) Thread.sleep(sleepBeforeExecute);
        stopCount++ ;
        DataflowEvent stopEvent = stopCount % 2 == 0 ? DataflowEvent.PAUSE : DataflowEvent.STOP;
        ExecuteLog stopExecuteLog = doStop(dflClient, stopEvent) ;
        executeLogs.add(stopExecuteLog);
        shell.console().println(stopExecuteLog.getFormatText());
        if(!stopExecuteLog.isSuccess()) {
          break;
        }
        stopCompleteCount++ ;

        if(sleepBeforeExecute > 0) Thread.sleep(sleepBeforeExecute);

        resumeCount++ ;
        ExecuteLog resumeExecuteLog = doResume(dflClient) ;
        executeLogs.add(resumeExecuteLog);
        shell.console().println(resumeExecuteLog.getFormatText());
        if(!resumeExecuteLog.isSuccess()) {
          break;
        }
        resumeCompleteCount++ ;
      }
    } catch(Exception ex) {
      ex.printStackTrace();
    }
    
    if(printSummary) {
      TabularFormater formater = new TabularFormater("#", "Description", "Duration", "Success") ;
      formater.setTitle("Start/Pause/Stop/Resume Test Summary");
      for(int i = 0; i < executeLogs.size(); i++) {
        ExecuteLog sel = executeLogs.get(i) ;
        Object[] cells = {
          (i + 1), sel.getDescription(), (sel.getStop() - sel.getStart()) + "ms", sel.isSuccess() 
        };
        formater.addRow(cells);
      }
      shell.console().println(formater.getFormatText());
      shell.console().println(
          "stop = " + stopCount +", stop complete = " + stopCompleteCount + ", " +
          "resume = " + resumeCount + ", resume complete = " + resumeCompleteCount);
    }
    
    if(junitReportFile != null) {
      //TODO: look into this junitReport and implement it
      junitReport(junitReportFile, executeLogs) ;
    }
  }
  
  ExecuteLog doStop(DataflowClient dflClient, DataflowEvent stopEvent) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Stop the dataflow with the event " + stopEvent) ;
    executeLog.start(); 
    DataflowLifecycleStatus expectStatus = DataflowLifecycleStatus.PAUSE;
    if(stopEvent == DataflowEvent.STOP) {
      expectStatus = DataflowLifecycleStatus.STOP;
    }
    WaitingOrderNodeEventListener stopWaitingListener = new WaitingOrderNodeEventListener(dflClient.getRegistry());
    stopWaitingListener.add(dflClient.getDataflowRegistry().getStatusNode().getPath(), expectStatus);
    dflClient.setDataflowEvent(stopEvent);
    stopWaitingListener.waitForEvents(maxWaitForStop);
    if(stopWaitingListener.getUndetectNodeEventCount() > 0) {
      executeLog.setSuccess(false);
    }
    executeLog.addLog(stopWaitingListener.getTabularFormaterEventLogInfo().getFormatText());
    executeLog.stop();
    return executeLog;
  }
  
  ExecuteLog doResume(DataflowClient dflClient) throws Exception {
    ExecuteLog executeLog = new ExecuteLog("Resume the dataflow") ;
    executeLog.start();
    WaitingOrderNodeEventListener resumeWaitingListener = new WaitingOrderNodeEventListener(dflClient.getRegistry());
    resumeWaitingListener.add(dflClient.getDataflowRegistry().getStatusNode().getPath(), DataflowLifecycleStatus.RUNNING);
    dflClient.setDataflowEvent(DataflowEvent.RESUME);
    resumeWaitingListener.waitForEvents(maxWaitForResume);
    if(resumeWaitingListener.getUndetectNodeEventCount() > 0) {
      executeLog.setSuccess(false);
    }
    executeLog.addLog(resumeWaitingListener.getTabularFormaterEventLogInfo().getFormatText());
    executeLog.stop();
    return executeLog ;
  }
}

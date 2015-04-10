package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.util.JSONSerializer;


public class HDFSDataflowTest extends DataflowTest {
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new HDFSDataflowSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new HDFSDataflowSinkValidator();
  
  protected void doRun(ScribenginShell shell) throws Exception {
    long start = System.currentTimeMillis();
    
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sourceGenerator.init(scribenginClient);
    sourceGenerator.run();
    
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-hdfs-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    //TODO: review this code
    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());
    
    dflDescriptor.addSinkDescriptor("default", sinkValidator.getSinkDescriptor());
    
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
    DataflowWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor) ;
    
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    try { 
      waitingEventListener.waitForEvents(duration);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
    sinkValidator.init(scribenginClient);
    sinkValidator.run();

    DataflowTestReport report = new DataflowTestReport() ;
    sourceGenerator.populate(report);
    sinkValidator.populate(report);
    shell.console().println(report.getFormatedReport());
    junitReport(report);
    shell.console().println("Test execution time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
  }
}
package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.util.JSONSerializer;


public class HdfsDataflowTest extends DataflowTest {
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new DataflowHDFSSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new DataflowHDFSSinkValidator();
  
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
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor) ;
    
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);

    sinkValidator.init(scribenginClient);
    sinkValidator.run();
    
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
  }
}
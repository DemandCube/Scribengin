package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.util.JSONSerializer;


public class KafkaToHdfsDataflowTest extends DataflowTest {

  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new KafkaDataflowSourceGenerator();
  
  
  protected void doRun(ScribenginShell shell) throws Exception {
    long start = System.currentTimeMillis();
    ScribenginClient scribenginClient = shell.getScribenginClient();

    sourceGenerator.init(scribenginClient);
    sourceGenerator.runInBackground();
    
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-kafka-hdfs-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());

    StorageDescriptor defaultSink = new StorageDescriptor("HDFS", getDataDir() + "/sink");
    dflDescriptor.addSinkDescriptor("default", defaultSink);
    
    StorageDescriptor invalidSink = new StorageDescriptor("HDFS", getDataDir() + "/invalid-sink");
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
   
    DataflowWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();

    DataflowTestReport report = new DataflowTestReport() ;
    sourceGenerator.populate(report);
    report.report(System.out);
    //TODO: Implemement and test the juniReport method
    junitReport(report);
    
  }

  private String getDataDir() {
    return "./build/hdfs";
  }
}
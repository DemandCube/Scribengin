package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;


public class HDFSToKafkaDataflowTest extends DataflowTest {

  public static final String TEST_NAME = "hdfs-kafka";

  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new HDFSDataflowSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new KafkaDataflowSinkValidator();
  
  protected void doRun(ScribenginShell shell) throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sourceGenerator.init(scribenginClient);
    sourceGenerator.run();

    sinkValidator.init(scribenginClient);

    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName(dataflowName);
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());

    dflDescriptor.addSinkDescriptor("default", sinkValidator.getSinkDescriptor());

    printDebugInfo(shell, scribenginClient, dflDescriptor);
    
    DataflowWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    try {
      waitingEventListener.waitForEvents(duration);
    } catch (Exception e) {
    }
    report(shell, waitingEventListener);
    
    sinkValidator.setExpectRecords(sourceGenerator.maxRecordsPerStream * sourceGenerator.numberOfStream);
    sinkValidator.run();
    //runInBackground() wont work

    report(shell, sourceGenerator, sinkValidator);

    if (dumpRegistry) {
      shell.execute("registry dump");
    }
  }
}
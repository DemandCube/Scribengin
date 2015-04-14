package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;

public class KafkaDataflowTest extends DataflowTest {
  final static public String TEST_NAME = "kafka-to-kakfa" ;
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new KafkaDataflowSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new KafkaDataflowSinkValidator();
  
  protected void doRun(ScribenginShell shell) throws Exception {
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sourceGenerator.init(scribenginClient);
    sourceGenerator.runInBackground();
    
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
    
    sinkValidator.setExpectRecords(sourceGenerator.getNumberOfGeneratedRecords());
    sinkValidator.run();
    sinkValidator.waitForTermination();
    
    report(shell, sourceGenerator, sinkValidator) ;
    if(dumpRegistry) {
      shell.execute("registry dump");
    }
  }


}
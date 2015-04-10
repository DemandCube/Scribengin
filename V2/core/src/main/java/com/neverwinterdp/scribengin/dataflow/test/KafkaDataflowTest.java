package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.event.DataflowWaitingEventListener;


public class KafkaDataflowTest extends DataflowTest {
  final static public String TEST_NAME = "kafka-to-kakfa" ;
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new KafkaDataflowSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new KafkaDataflowSinkValidator();
  
  @Parameter(names = "--sink-topic", description = "Default sink topic")
  public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  
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
    
    if(debugDataflowTask) {
      RegistryDebugger taskDebugger = shell.getScribenginClient().getDataflowTaskDebugger(System.out, dataflowName) ;
    }
    if(debugDataflowWorker) {
      RegistryDebugger workerDebugger = shell.getScribenginClient().getDataflowWorkerDebugger(System.out, dataflowName) ;
    }
    DataflowWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    
    waitingEventListener.waitForEvents(duration);
    dataflowInfoThread.interrupt();

    report(shell, waitingEventListener) ;
    
    sinkValidator.setExpectRecords(sourceGenerator.getNumberOfGeneratedRecords());
    sinkValidator.run();
    sinkValidator.waitForTermination();
    
    report(shell, sourceGenerator, sinkValidator) ;
  }
}
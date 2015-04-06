package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;


public class KafkaDataflowTest extends DataflowTest {
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new DataflowKafkaSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator   = new DataflowKafkaSinkValidator();
  
  @Parameter(names = "--sink-topic", description = "Default sink topic")
  public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  
  protected void doRun(ScribenginShell shell) throws Exception {
    shell.console().println("KafkaDataflowTest: Prepare to launch the KafkaDataflowTest!!");
    long start = System.currentTimeMillis();
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sourceGenerator.init(scribenginClient);
    sourceGenerator.runInBackground();
    shell.console().println("KafkaDataflowTest: Finish launching the source generator in the background!!");
    
    sinkValidator.init(scribenginClient);
    
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());
    dflDescriptor.addSinkDescriptor("default", sinkValidator.getSinkDescriptor());
    shell.console().println("Finish creating the dataflow descriptor!!!");
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    shell.console().println("Finish submitting the dataflow descriptor!!!");
    shell.console().println("Wait time to finish: " + duration + "ms");
    
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    
    dataflowInfoThread.interrupt();
    
    
    sinkValidator.setExpectRecords(sourceGenerator.getNumberOfGeneratedRecords());
    sinkValidator.run();
    sinkValidator.waitForTermination();
    DataflowTestReport report = new DataflowTestReport() ;
    sourceGenerator.populate(report);
    sinkValidator.populate(report);
    report.report(System.out);

    junitReport(report);
  }

  static public class TestCopyScribe extends ScribeAbstract {
    private int count = 0;
    
    @Override
    public void process(Record record, DataflowTaskContext ctx) throws Exception {
      ctx.append(record);
      count++ ;
      if(count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }
}
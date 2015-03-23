package com.neverwinterdp.scribengin.dataflow.test;

import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.kafka.tool.KafkaMessageGenerator;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.util.JSONSerializer;


public class KafkaDataflowTest extends DataflowTest {
  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new DataflowKafkaSourceGenerator();
  
  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator = new DataflowKafkaSinkValidator();
  
  @Parameter(names = "--sink-topic", description = "Default sink topic")
  public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  
  protected void doRun(ScribenginShell shell) throws Exception {
    long start = System.currentTimeMillis();
    ScribenginClient scribenginClient = shell.getScribenginClient();

    sourceGenerator.init(scribenginClient);
    sourceGenerator.runInBackground();
    
    sinkValidator.init(scribenginClient);
    
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    dflDescriptor.setSourceDescriptor(sourceGenerator.getSourceDescriptor());
    dflDescriptor.addSinkDescriptor("default", sinkValidator.getSinkDescriptor());
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
    
    
    sinkValidator.run();
    sinkValidator.waitForTermination();
    DataflowTestReport report = new DataflowTestReport() ;
    sourceGenerator.populate(report);
    sinkValidator.populate(report);
    report.report(System.out);
    //TODO: Implemement and test the juniReport method
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
  
  static public class KafkaMessageGeneratorRecord implements KafkaMessageGenerator {
    static public AtomicLong idTracker = new AtomicLong() ;
    
    public byte[] nextMessage(int partition, int messageSize) {
      String key = "partition=" + partition + ",id=" + idTracker.getAndIncrement();
      return JSONSerializer.INSTANCE.toString(new Record(key, new byte[messageSize] )).getBytes();
    }
  }
}
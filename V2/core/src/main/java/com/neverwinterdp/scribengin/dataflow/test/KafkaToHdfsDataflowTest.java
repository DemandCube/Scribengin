package com.neverwinterdp.scribengin.dataflow.test;

import java.util.Random;

import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.util.JSONSerializer;


public class KafkaToHdfsDataflowTest extends DataflowTest {

  @ParametersDelegate
  private DataflowSourceGenerator sourceGenerator = new DataflowKafkaSourceGenerator();
  
  
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
   
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
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
  
  static public class TestCopyScribe extends ScribeAbstract {
    private int count = 0;
    private Random random = new Random();
    
    @Override
    public void process(Record record, DataflowTaskContext ctx) throws Exception {
      if(random.nextDouble() < 0.8) {
        ctx.append(record);
        System.out.println("Write default");
      } else {
        ctx.write("invalid", record);
        System.out.println("Write invalid");
      }
      count++ ;
      if(count == 100) {
        ctx.commit();
        count = 0;
      }
    }
  }

}
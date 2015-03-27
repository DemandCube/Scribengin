package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskContext;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.scribe.ScribeAbstract;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.HDFSSourceGenerator;
import com.neverwinterdp.util.JSONSerializer;


public class HdfsToKafkaDataflowTest extends DataflowTest {

  @ParametersDelegate
  private DataflowSinkValidator   sinkValidator = new DataflowKafkaSinkValidator();
  
  @Parameter(names = "--sink-topic", description = "Default sink topic")
  public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  
  protected void doRun(ScribenginShell shell) throws Exception {
    long start = System.currentTimeMillis();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    ScribenginClient scribenginClient = shell.getScribenginClient();
    sinkValidator.init(scribenginClient);
    
    new HDFSSourceGenerator().generateSource(fs, getDataDir() + "/source");
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-hdfs-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());
    StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", getDataDir() + "/source") ;
    dflDescriptor.setSourceDescriptor(storageDescriptor);

    dflDescriptor.addSinkDescriptor("default", sinkValidator.getSinkDescriptor());
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
   
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);

    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
    
    
    //sinkValidator.setExpectRecords(sourceGenerator.getNumberOfGeneratedRecords());
    //sinkValidator.run();
    //sinkValidator.waitForTermination();
    DataflowTestReport report = new DataflowTestReport() ;
    //sourceGenerator.populate(report);
    //sinkValidator.populate(report);
    report.report(System.out);
    //TODO: Implemement and test the juniReport method
    junitReport(report);
  }

  private String getDataDir() {
    return "./build/hdfs";
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
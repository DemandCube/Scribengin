package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.test.HelloHDFSToKafkaDataflowBuilder.TestCopyScribe;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.HDFSSourceGenerator;
import com.neverwinterdp.util.JSONSerializer;


public class HdfsToKafkaDataflowTest extends DataflowTest {

  @Parameter(names = "--sink-topic", description = "Default sink topic")
  public String DEFAULT_SINK_TOPIC = "hello.sink.default" ;
  
  @Parameter(names = "--invalidsink-topic", description = "Invalid sink topic")
  public String INVALID_SINK_TOPIC = "hello.sink.invalid" ;
  
  @Parameter(names = "--flow-name", description = "Invalid sink topic")
  private String name                   = "hello";
  
  @Parameter(names = "--kafka-write-period", description = "The write period for each partition in ms")
  private int writePeriod = 10;
  
  @Parameter(names = "--kafka-num-partition", description = "Number of the partitions")
  private int numPartitions = 5;
  
  protected void doRun(ScribenginShell shell) throws Exception {
    long start = System.currentTimeMillis();
    FileSystem fs = FileSystem.getLocal(new Configuration());
    new HDFSSourceGenerator().generateSource(fs, getDataDir() + "/source");
    ScribenginClient scribenginClient = shell.getScribenginClient();
    RegistryConfig registryConfig = scribenginClient.getRegistry().getRegistryConfig();
    String zkConnect = registryConfig.getConnect();
    KafkaTool client = new KafkaTool(name, zkConnect) ;
    client.connect();
    String brokerList = client.getKafkaBrokerList() ;
    client.close();
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-hdfs-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());
    StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", getDataDir() + "/source") ;
    dflDescriptor.setSourceDescriptor(storageDescriptor);

    StorageDescriptor defaultSink = new StorageDescriptor("KAFKA");
    defaultSink.attribute("name", name);
    defaultSink.attribute("topic", DEFAULT_SINK_TOPIC);
    defaultSink.attribute("zk.connect", zkConnect);
    defaultSink.attribute("broker.list", brokerList);
    dflDescriptor.addSinkDescriptor("default", defaultSink);

    StorageDescriptor invalidSink = new StorageDescriptor("KAFKA");
    invalidSink.attribute("name", name);
    invalidSink.attribute("topic", INVALID_SINK_TOPIC);
    invalidSink.attribute("zk.connect", zkConnect);
    invalidSink.attribute("broker.list", brokerList);
    dflDescriptor.addSinkDescriptor("invalid", invalidSink);
    System.out.println(JSONSerializer.INSTANCE.toString(dflDescriptor)) ;
   
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
   
    
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
  }

  private String getDataDir() {
    return "./build/hdfs";
  }
}
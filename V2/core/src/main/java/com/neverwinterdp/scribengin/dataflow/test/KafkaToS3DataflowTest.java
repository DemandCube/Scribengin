package com.neverwinterdp.scribengin.dataflow.test;

import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaMessageGenerator;
import com.neverwinterdp.kafka.tool.KafkaMessageSendTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.test.HelloKafkaDataflowBuilder.TestCopyScribe;
import com.neverwinterdp.scribengin.event.ScribenginWaitingEventListener;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.util.JSONSerializer;


public class KafkaToS3DataflowTest extends DataflowTest {
  @Parameter(names = "--source-topic", description = "Source topic")
  public String SOURCE_TOPIC       = "hello.source" ;
  
  @Parameter(names = "--flow-name", description = "Invalid sink topic")
  private String name                   = "hello";
  
  @Parameter(names = "--kafka-write-period", description = "The write period for each partition in ms")
  private int writePeriod = 10;
  
  @Parameter(names = "--kafka-num-partition", description = "Number of the partitions")
  private int numPartitions = 5;
  
  @Parameter(names = "--kafka-max-message-per-partition", description = "Number of the partitions")
  private int maxMessagePerPartition = 100;
  
  static public String BUCKET_NAME = "sink-source-test";
  static public String STORAGE_PATH = "database";
  
  protected void doRun(ScribenginShell shell) throws Exception {
    
    
    
    long start = System.currentTimeMillis();
    ScribenginClient scribenginClient = shell.getScribenginClient();
    RegistryConfig registryConfig = scribenginClient.getRegistry().getRegistryConfig();
    String zkConnect = registryConfig.getConnect();
    
    
    String[] sendArgs = {"--topic", SOURCE_TOPIC, 
                     "--send-period", Integer.toString(writePeriod),
                     "--num-partition", Integer.toString(numPartitions),
                     "--send-max-per-partition", Integer.toString(maxMessagePerPartition),
                     "--send-max-duration", Long.toString(duration),
                     "--zk-connect", zkConnect};
    KafkaMessageSendTool sendTool = new KafkaMessageSendTool();
    new JCommander(sendTool, sendArgs);
    sendTool.setMessageGenerator(new KafkaMessageGeneratorRecord());
    sendTool.runAsDeamon();

    KafkaTool client = new KafkaTool(name, zkConnect) ;
    client.connect();
    String brokerList = client.getKafkaBrokerList() ;
    client.close();

    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("hello-kafka-dataflow");
    dflDescriptor.setNumberOfWorkers(numOfWorkers);
    dflDescriptor.setTaskMaxExecuteTime(taskMaxExecuteTime);
    dflDescriptor.setNumberOfExecutorsPerWorker(numOfExecutorPerWorker);
    dflDescriptor.setScribe(TestCopyScribe.class.getName());

    StorageDescriptor storageDescriptor = new StorageDescriptor("KAFKA") ;
    storageDescriptor.attribute("name", name);
    storageDescriptor.attribute("topic", SOURCE_TOPIC);
    storageDescriptor.attribute("zk.connect", zkConnect);
    storageDescriptor.attribute("broker.list", brokerList);
    dflDescriptor.setSourceDescriptor(storageDescriptor);

    StorageDescriptor defaultSink = new StorageDescriptor("S3");
    defaultSink.attribute("s3.bucket.name",  BUCKET_NAME);
    defaultSink.attribute("s3.storage.path", STORAGE_PATH);
    

    StorageDescriptor invalidSink = new StorageDescriptor("S3");
    invalidSink.attribute("name", name);

    
    ScribenginWaitingEventListener waitingEventListener = scribenginClient.submit(dflDescriptor);
    
     
    shell.console().println("Wait time to finish: " + duration + "ms");
    Thread dataflowInfoThread = newPrintDataflowThread(shell, dflDescriptor);
    dataflowInfoThread.start();
    waitingEventListener.waitForEvents(duration);
    shell.console().println("The test executed time: " + (System.currentTimeMillis() - start) + "ms");
    dataflowInfoThread.interrupt();
  }
  
  static public class KafkaMessageGeneratorRecord implements KafkaMessageGenerator {
    static public AtomicLong idTracker = new AtomicLong() ;
    
    public byte[] nextMessage(int partition, int messageSize) {
      String key = "partition=" + partition + ",id=" + idTracker.getAndIncrement();
      return JSONSerializer.INSTANCE.toString(new Record(key, new byte[messageSize] )).getBytes();
    }
  }

}